# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.
"""
Script for testing complete functionality of the MTurk conversation backend.
Simulates agents and interactions and tests the outcomes of interacting with
the server to ensure that the messages that are recieved are as intended.

It pretends to act in the way that core.html is supposed to follow, both
related to what is sent and recieved, what fields are checked, etc. A change
to the core.html file will not be caught by this script.

Doesn't actually interact with Amazon MTurk as they don't offer a robust
testing framework as of September 2017, so interactions with MTurk and updating
HIT status and things of the sort are not yet supported in this testing.
"""
from parlai.core.params import ParlaiParser
from parlai.mturk.tasks.qa_data_collection.worlds import TestOnboardWorld, TestSoloWorld
from parlai.mturk.core.mturk_manager import MTurkManager
from parlai.mturk.core.server_utils import setup_server, delete_server
from parlai.mturk.core.socket_manager import Packet, SocketManager
import parlai.mturk.core.data_model as data_model
from parlai.mturk.core.mturk_utils import create_hit_config
from socketIO_client_nexus import SocketIO
import time
import os
import importlib
import copy
import uuid
import threading
from itertools import product
from joblib import Parallel, delayed

TEST_TASK_DESCRIPTION = 'This is a test task description'
MTURK_AGENT_IDS = ['TEST_USER_1', 'TEST_USER_2']
PORT = 443
TASK_GROUP_ID = 'TEST_TASK_GROUP'
AGENT_1_ID = 'TEST_AGENT_1'
ASSIGN_1_ID = 'FAKE_ASSIGNMENT_ID_1'
HIT_1_ID = 'FAKE_HIT_ID_1'
SOLO_ONBOARDING_TEST = 'SOLO_ONBOARDING_TEST'

class MockAgent(object):
    """Class that pretends to be an MTurk agent interacting through the
    webpage by simulating the same commands that are sent from the core.html
    file. Exposes methods to use for testing and checking status
    """
    def __init__(self, opt, hit_id, assignment_id, worker_id):
        self.conversation_id = None
        self.id = None
        self.assignment_id = assignment_id
        self.hit_id = hit_id
        self.worker_id = worker_id
        self.some_agent_disconnected = False
        self.disconnected = False
        self.task_group_id = TASK_GROUP_ID
        self.socketIO = None

    def send_packet(self, packet_type, data, callback):
        if not callback:
            def callback(*args):
                pass

        msg = {
          'id': str(uuid.uuid4()),
          'type': packet_type,
          'sender_id': self.worker_id,
          'assignment_id': self.assignment_id,
          'conversation_id': self.conversation_id,
          'receiver_id': '[World_' + self.task_group_id + ']',
          'data': data
        }

        event_name = data_model.SOCKET_ROUTE_PACKET_STRING
        if (packet_type == Packet.TYPE_ALIVE):
            event_name = data_model.SOCKET_AGENT_ALIVE_STRING

        self.socketIO.emit(event_name, msg, callback)

    def send_message(self, text, callback):
        if not callback:
            def callback(*args):
                pass

        data = {
            'text': text,
            'id': self.id,
            'message_id': str(uuid.uuid4()),
            'episode_done': False
        }

        self.send_packet(Packet.TYPE_MESSAGE, data, callback)

    def setup_socket(self, server_url, message_handler):
        """Sets up a socket for an agent"""
        def on_socket_open(*args):
            data = {
                'hit_id': self.hit_id,
                'assignment_id': self.assignment_id,
                'worker_id': self.worker_id,
                'conversation_id': self.conversation_id
            }
            self.send_packet(Packet.TYPE_ALIVE, data, None)
        def on_new_message(*args):
            message_handler(args[0])
        def on_disconnect(*args):
            self.disconnected = True
        self.socketIO = SocketIO(server_url, PORT)
        # Register Handlers
        self.socketIO.on(data_model.SOCKET_OPEN_STRING, on_socket_open)
        self.socketIO.on(data_model.SOCKET_DISCONNECT_STRING, on_disconnect)
        self.socketIO.on(data_model.SOCKET_NEW_PACKET_STRING, on_new_message)

        # Start listening thread
        self.listen_thread = threading.Thread(target=self.socketIO.wait)
        self.listen_thread.daemon = True
        self.listen_thread.start()

    def send_heartbeat(self):
        """Sends a heartbeat to the world"""
        hb = {
            'id': str(uuid.uuid4()),
            'receiver_id': '[World_' + self.task_group_id + ']',
            'assignment_id': self.assignment_id,
            'sender_id' : self.worker_id,
            'conversation_id': self.conversation_id,
            'type': Packet.TYPE_HEARTBEAT,
            'data': None
        }
        self.socketIO.emit(data_model.SOCKET_ROUTE_PACKET_STRING, hb)


def handle_setup(opt):
    """Prepare the heroku server without creating real hits"""
    create_hit_config(
        task_description=TEST_TASK_DESCRIPTION,
        unique_worker=False,
        is_sandbox=True
    )
    # Poplulate files to copy over to the server
    task_files_to_copy = []
    task_directory_path = os.path.join(
        opt['parlai_home'],
        'parlai',
        'mturk',
        'core',
        'test',
        'integration_test'
    )
    task_files_to_copy.append(
        os.path.join(task_directory_path, 'html', 'cover_page.html'))
    for mturk_agent_id in MTURK_AGENT_IDS + ['onboarding']:
        task_files_to_copy.append(os.path.join(
            task_directory_path,
            'html',
            '{}_index.html'.format(mturk_agent_id)
        ))

    # Setup the server with a likely-unique app-name
    task_name = '{}-{}'.format(str(uuid.uuid4())[:8], 'integration_test')
    server_task_name = \
        ''.join(e for e in task_name if e.isalnum() or e == '-')
    server_url = \
        setup_server(server_task_name, task_files_to_copy)

    return server_task_name, server_url


def handle_shutdown(server_task_name):
    delete_server(server_task_name)


def test_socket_manager(opt, server_url):
    TEST_MESSAGE = 'This is a test'
    socket_manager = None
    world_received_alive = False
    world_received_message = False
    agent_timed_out = False

    def world_on_alive(pkt):
        nonlocal world_received_alive
        # Assert alive packets contain the right data
        worker_id = pkt.data['worker_id']
        assert worker_id == AGENT_1_ID, 'Worker id was {}'.format(worker_id)
        hit_id = pkt.data['hit_id']
        assert hit_id == HIT_1_ID, 'HIT id was {}'.format(hit_id)
        assign_id = pkt.data['assignment_id']
        assert assign_id == ASSIGN_1_ID, 'Assign id was {}'.format(assign_id)
        conversation_id = pkt.data['conversation_id']
        assert conversation_id == None, \
            'Conversation id was {}'.format(conversation_id)
        # Start a channel
        socket_manager.open_channel(worker_id, assign_id)
        # Note that alive was successful
        world_received_alive = True

    def world_on_new_message(pkt):
        nonlocal world_received_message
        text = pkt.data['text']
        assert text == TEST_MESSAGE, 'Received text was {}'.format(text)
        world_received_message = True

    def world_on_socket_dead(worker_id, assign_id):
        nonlocal agent_timed_out
        assert worker_id == AGENT_1_ID, 'Worker id was {}'.format(worker_id)
        assert assign_id == ASSIGN_1_ID, 'Assign id was {}'.format(assign_id)
        agent_timed_out = True
        return True

    socket_manager = SocketManager(
        server_url,
        PORT,
        world_on_alive,
        world_on_new_message,
        world_on_socket_dead,
        TASK_GROUP_ID
    )

    agent_got_response_heartbeat = False
    def agent_on_message(pkt):
        nonlocal agent_got_response_heartbeat
        if pkt['type'] == Packet.TYPE_HEARTBEAT:
            agent_got_response_heartbeat = True

    agent = MockAgent(opt, HIT_1_ID, ASSIGN_1_ID, AGENT_1_ID)
    agent.setup_socket(server_url, agent_on_message)
    time.sleep(1)
    agent.send_heartbeat()
    time.sleep(1)
    agent.send_message(TEST_MESSAGE, None)
    time.sleep(10)
    assert world_received_alive, 'World never received alive message'
    assert world_received_message, 'World never received test message'
    assert agent_timed_out, 'Agent did not timeout'
    assert agent_got_response_heartbeat, 'Agent never got response heartbeat'
    print('Socket Manager test passed')


def run_solo_world(opt, mturk_manager, is_onboarded):
    # Runs the solo test world with or without onboarding
    def run_onboard(worker):
        world = TestOnboardWorld(opt=opt, mturk_agent=worker)
        while not world.episode_done():
            world.parley()
        world.shutdown()

    if is_onboarded:
        mturk_manager.set_onboard_function(onboard_function=run_onboard)
    else:
        mturk_manager.set_onboard_function(onboard_function=None)

    try:
        mturk_manager.start_new_run()
        mturk_manager.ready_to_accept_workers()

        def check_worker_eligibility(worker):
            return True

        def get_worker_role(worker):
            return mturk_agent_id

        global run_conversation
        def run_conversation(mturk_manager, opt, workers):
            task = SOLO_ONBOARDING_TEST
            mturk_agent = workers[0]
            world = TestSoloWorld(opt=opt, task=task, mturk_agent=mturk_agent)
            while not world.episode_done():
                world.parley()
            world.shutdown()
            world.review_work()

        mturk_manager.start_task(
            eligibility_function=check_worker_eligibility,
            role_function=get_worker_role,
            task_function=run_conversation
        )
    except:
        raise
    finally:
        mturk_manager.expire_all_unassigned_hits()

def setup_duo_world(opt, server_url, is_onboarded):
    pass

def test_solo_with_onboarding(opt, server_url):
    opt['task'] = SOLO_ONBOARDING_TEST

    mturk_agent_id = AGENT_1_ID
    mturk_manager = MTurkManager(
        opt=opt,
        mturk_agent_ids = [mturk_agent_id]
    )
    mturk_manager.server_url = server_url
    world_thread = threading.Thread(target=run_solo_world,
                                    args=(opt, mturk_manager, is_onboarded))

    # Act as the agents
    # Assert that they get through as fast as possible

def test_solo_no_onboarding(opt, server_url):
    pass

def test_solo_refresh_after_complete(opt, server_url):
    pass

def test_solo_refresh_in_middle(opt, server_url):
    pass

def test_duo_with_onboarding(opt, server_url):
    pass

def test_duo_no_onboarding(opt, server_url):
    pass

def test_duo_valid_reconnects(opt, server_url):
    pass

def test_duo_one_disconnect(opt, server_url):
    pass

def test_solo_expired_reconnects(opt, server_url):
    pass

def test_count_complete(opt, server_url):
    # Should test count_complete and also ensure that two worlds can
    # start even if just one was requested
    pass

def test_count_started(opt, server_url):
    # Should test count_complete and also ensure that workers in onboarding
    # or in waiting get expired
    pass

def test_allowed_conversations(opt, server_url):
    # Ensure that one worker can't get in two conversations at once with
    # just one allowed concurrent conversation
    pass

def test_unique_workers_in_conversation(opt, server_url):
    # Ensure that no task world (out of sandbox) can have the same worker
    # twice
    pass

def main():
    argparser = ParlaiParser(False, False)
    argparser.add_parlai_data_path()
    argparser.add_mturk_args()
    opt = argparser.parse_args()
    opt['sandbox'] = True
    task_name, server_url = handle_setup(opt)
    try:
        test_socket_manager(opt, server_url)
    except:
        raise
    finally:
        handle_shutdown(task_name)
    #
    # opt['task'] = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
    # opt.update(task_config)
    #
    # # Initialize a SQuAD teacher agent, which we will get context from
    # module_name = 'parlai.tasks.squad.agents'
    # class_name = 'DefaultTeacher'
    # my_module = importlib.import_module(module_name)
    # task_class = getattr(my_module, class_name)
    # task_opt = {}
    # task_opt['datatype'] = 'train'
    # task_opt['datapath'] = opt['datapath']
    #
    # mturk_agent_id = 'Worker'
    # mturk_manager = MTurkManager(
    #     opt=opt,
    #     mturk_agent_ids = [mturk_agent_id]
    # )
    # mturk_manager.setup_server()
    #
    # def run_onboard(worker):
    #     world = QADataCollectionOnboardWorld(opt=opt, mturk_agent=worker)
    #     while not world.episode_done():
    #         world.parley()
    #     world.shutdown()
    #
    # mturk_manager.set_onboard_function(onboard_function=None)
    #
    # try:
    #     mturk_manager.start_new_run()
    #     mturk_manager.create_hits()
    #
    #     mturk_manager.ready_to_accept_workers()
    #
    #     def check_worker_eligibility(worker):
    #         return True
    #
    #     def get_worker_role(worker):
    #         return mturk_agent_id
    #
    #     global run_conversation
    #     def run_conversation(mturk_manager, opt, workers):
    #         task = task_class(task_opt)
    #         mturk_agent = workers[0]
    #         world = QADataCollectionWorld(opt=opt, task=task, mturk_agent=mturk_agent)
    #         while not world.episode_done():
    #             world.parley()
    #         world.shutdown()
    #         world.review_work()
    #
    #     mturk_manager.start_task(
    #         eligibility_function=check_worker_eligibility,
    #         role_function=get_worker_role,
    #         task_function=run_conversation
    #     )
    # except:
    #     raise
    # finally:
    #     mturk_manager.expire_all_unassigned_hits()
    #     mturk_manager.shutdown()

if __name__ == '__main__':
    main()
