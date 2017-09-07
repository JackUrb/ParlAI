# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.
from parlai.core.worlds import validate
from parlai.mturk.core.worlds import MTurkOnboardWorld, MTurkTaskWorld

class TestOnboardWorld(MTurkOnboardWorld):
    TEST_ID = 'ONBOARD_SYSTEM'
    TEST_TEXT_1 = 'FIRST_ONBOARD_MESSAGE'
    TEST_TEXT_2 = 'SECOND_ONBOARD_MESSAGE'

    def parley(self):
        ad = {}
        ad['id'] = TEST_ID
        ad['text'] = TEST_TEXT_1
        self.mturk_agent.observe(ad)
        response = self.mturk_agent.act()
        self.mturk_agent.observe({
            'id': TEST_ID,
            'text': TEST_TEXT_2
        })
        self.episodeDone = True


class TestSoloWorld(MTurkTaskWorld):
    """
    World for taking 2 turns and then marking the worker as done
    """

    TEST_ID = 'SYSTEM'
    TEST_TEXT_1 = 'FIRST_MESSAGE'
    TEST_TEXT_2 = 'SECOND_MESSAGE'

    def __init__(self, opt, task, mturk_agent):
        self.task = task
        self.mturk_agent = mturk_agent
        self.episodeDone = False
        self.turn_index = -1

    def parley(self):
        self.turn_index = (self.turn_index + 1) % 2
        ad = { 'episode_done': False }
        ad['id'] = self.__class__.TEST_ID

        if self.turn_index == 0:
            # Take a first turn
            ad['text'] = TEST_TEXT_1

            self.mturk_agent.observe(validate(ad))
            self.response1 = self.mturk_agent.act()

        if self.turn_index == 1:
            # Complete after second turn
            ad['text'] = TEST_TEXT_2

            ad['episode_done'] = True  # end of episode

            self.mturk_agent.observe(validate(ad))
            self.response2 = self.mturk_agent.act()

            self.episodeDone = True

    def episode_done(self):
        return self.episodeDone

    def report(self):
        pass

    def shutdown(self):
        # Test runner should handle the shutdown
        pass

    def review_work(self):
        pass

class TestDuoWorld(MTurkTaskWorld):
    """World where 3 participants send messages in a circle until one marks the
    episode as done
    """
    def __init__(self, opt, agents=None, shared=None):
        # Add passed in agents directly.
        self.agents = agents
        self.acts = [None] * len(agents)
        self.episodeDone = False

    def parley(self):
        """For each agent, act, then force other agents to observe your act
        """
        acts = self.acts
        for index, agent in enumerate(self.agents):
            try:
                acts[index] = agent.act(timeout=None)
            except TypeError:
                acts[index] = agent.act() # not MTurkAgent
            if acts[index]['episode_done']:
                self.episodeDone = True
            for other_agent in self.agents:
                if other_agent != agent:
                    other_agent.observe(validate(acts[index]))

    def episode_done(self):
        return self.episodeDone

    def shutdown(self):
        # Test runner should handle the shutdown
        pass
