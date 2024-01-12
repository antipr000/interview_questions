"""
Question:
Design an IPL scoreboard similar to https://m.cricbuzz.com/live-cricket-scorecard/45886/csk-vs-kkr-1st-match-indian-premier-league-2022
Assume you are getting data streams from a provider (csv rows for us)
How would you model the problem and process the streams?
"""


import csv
from abc import ABC, abstractmethod
from typing import List


class BattingBehaviour:
    def __init__(self):
        self.runs_scored = 0
        self.balls_faced = 0
        self.boundary_types = [4, 6]
        self.boundaries = {}

    def add_run(self, run):
        self.runs_scored += run
        self.balls_faced += 1
        if run in self.boundary_types:
            self.boundaries[run] += 1

    def __repr__(self):
        return {
            'runs_scored': self.runs_scored,
            'balls_faced': self.balls_faced,
            'boundaries': self.boundaries
        }




class BowlingBehaviour:
    def __init__(self):
        self.wickets_taken = 0
        self.runs_accumulated = 0
        self.balls_done = 0
        self.wides = 0
        self.no_balls = 0

    def add_ball(self):
        self.balls_done += 1

    def add_wicket(self):
        self.wickets_taken += 1

    def add_run(self, run):
        self.runs_accumulated += run

    def calculate_economy(self):
        return self.runs_accumulated / self.balls_done

    def add_extra(self, extra_type, run):
        if extra_type == 'wides':
            self.wides += run
        elif extra_type == 'noballs':
            self.no_balls += run
        self.add_run(run)

    def __repr__(self):
        return {
            'runs_accumulated': self.runs_accumulated,
            'wides': self.wides,
            'noballs': self.no_balls,
            'wickets_taken': self.wickets_taken,
            'balls_done': self.balls_done,
        }

class Player:
    def __init__(self, name):
        self.name = name
        self.batting_behaviour = BattingBehaviour()
        self.bowling_behaviour = BowlingBehaviour()

    def __repr__(self):
        return {
            'name': self.name,
            'batting': self.batting_behaviour.__repr__(),
            'bowling': self.bowling_behaviour.__repr__()
        }

class Team:
    def __init__(self):
        self.players = []
        self.extras = {}
        self.runs_scored = 0
        self.wickets = 0

    def add_player(self, player):
        self.players.append(player)

    def add_run(self, run):
        self.runs_scored += run

    def add_extras(self, extra_type, run):
        if extra_type not in self.extras:
            self.extras[extra_type] = run
        else:
            self.extras[extra_type] += run

    def add_wicket(self):
        self.wickets += 1

    def __repr__(self):
        return {
            "runs_scored": self.runs_scored,
            "wickets": self.wickets,
            "extras": self.extras,
            "players": [player.__repr__() for player in self.players]
        }


class OverCalculator(ABC):
    @abstractmethod
    def calculate(self, balls):
        pass


class SixBallOverCalculator(OverCalculator):
    def calculate(self, balls):
        overs = balls // 6
        balls_curr_over = balls % 6
        return {
            'overs': overs,
            'balls': balls_curr_over,
        }


def get_over_calculator(num_balls) -> OverCalculator:
    if num_balls == 6:
        return SixBallOverCalculator()


class ScoreCard(object):
    def __init__(self):
        self.balls_per_over = None
        self.date = None
        self.venue = None
        self.city = None
        self.batting_team: Team = None
        self.bowling_team: Team = None
        self.striker: Player = None
        self.non_striker: Player = None
        self.current_bowler: Player = None
        self.balls = 0

    def set_balls_per_over(self, balls):
        self.balls_per_over = balls

    def set_date(self, date):
        self.date = date

    def venue(self, venue):
        self.venue = venue

    def set_batting_team(self, batting_team):
        self.batting_team = batting_team

    def set_bowling_team(self, bowling_team):
        self.bowling_team = bowling_team

    def add_run(self, run):
        self.batting_team.add_run(run)
        self.striker.batting_behaviour.add_run(run)
        self.current_bowler.bowling_behaviour.add_run(run)
        self.current_bowler.bowling_behaviour.add_ball()

    def add_extra(self, extra_type, run):
        self.batting_team.add_extras(extra_type, run)
        self.bowling_team.add_extras(extra_type, run)
        self.batting_team.add_run(run)
        self.current_bowler.bowling_behaviour.add_extra(extra_type, run)

    def add_wicket(self):
        self.batting_team.add_wicket()
        self.bowling_team.add_wicket()
        self.current_bowler.bowling_behaviour.add_wicket()

    def add_ball(self):
        self.balls += 1
        self.current_bowler.bowling_behaviour.add_ball()

    def get_overs(self):
        over_calculator = get_over_calculator(self.balls_per_over)
        return over_calculator.calculate(self.balls)

    def set_current_players(self, striker_name, non_striker_name, bowler_name):
        for striker in self.batting_team.players:
            if striker.name == striker_name:
                self.striker = striker

        for non_striker in self.batting_team.players:
            if non_striker.name == non_striker_name:
                self.non_striker = non_striker

        for bowler in self.bowling_team.players:
            if bowler.name == bowler_name:
                self.current_bowler = bowler

    def __repr__(self):
        return {
            'date': self.date,
            'venue': self.venue,
            'city': self.city,
            'batting_team': self.batting_team.__repr__(),
            'bowling_team': self.bowling_team.__repr__(),
            'striker': self.striker.__repr__(),
            'non_striker': self.non_striker.__repr__(),
            'bowler': self.current_bowler.__repr__(),
            'overs': self.get_overs()
        }


class RunCalculator(ABC):
    @abstractmethod
    def match(self, run_info):
        pass

    @abstractmethod
    def calculate_run(self, scorecard: ScoreCard, run_info):
        pass


class NormalRunCalculator(RunCalculator):

    def match(self, run_info):
        if int(run_info['runs_off_bat']) > 0:
            return True
        return False

    def calculate_run(self, scorecard: ScoreCard, run_info):
        scorecard.add_run(int(run_info['runs_off_bat']))


class ExtraRunCalculator(RunCalculator):
    def match(self, run_info):
        if int(run_info['extras']) > 0:
            return True
        return False

    def calculate_run(self, scorecard: ScoreCard, run_info):
        """
            Note: We can use factory here for different extras and have custom logic
            Example of use could be:
            if type == 'wide':
                return WideImpl()

            if extra_impl.match(self, run_info):
                extra_impl.add(self, team, run_info)
        """
        extra_types = ['wides', 'noballs', 'byes', 'legbyes', 'penalty']
        for extra_type in extra_types:
            if run_info[extra_type] != '':
                scorecard.add_extra(extra_type, int(run_info[extra_type]))


class WicketCalculator(RunCalculator):
    def match(self, run_info):
        if run_info['wicket_type'] != '':
            return True
        return False

    def calculate_run(self, scorecard: ScoreCard, run_info):
        scorecard.add_wicket()


def get_run_calculators() -> List[RunCalculator]:
    return [NormalRunCalculator(), ExtraRunCalculator(), WicketCalculator()]

class Matches(object):
    _instance = None
    matches = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Matches, cls).__new__(cls)
            cls.matches = {}
        return cls._instance

    def get_scorecard(self, match_id):
        if self.matches.get(match_id, None):
            return self.matches[match_id]
        self.matches[match_id] = ScoreCard()
        return self.matches[match_id]


class Parser(ABC):
    @abstractmethod
    def parse(self, request):
        pass


class ScoreEntryParser(Parser):
    """
    Parser for score entries, we use it to parse each score line.
    We use matchers in this case
    """
    def make_dict(self, request):
        "Internal method to create a dictionary out of the row data"
        return {}

    def parse(self, request):
        """

        :param request:
        :return: Updated scorecard repr
        """
        run_info = self.make_dict(request)
        match = Matches()
        scorecard = match.get_scorecard(run_info["match_id"])
        run_calculators = get_run_calculators()
        for run_calculator in run_calculators:
            if run_calculator.match(run_info):
                run_calculator.calculate_run(scorecard, run_info)

        return scorecard.__repr__()


class InfoEntryParser(Parser):
    """
    Parser for infos. We use this to create new scorecard and teams and players
    """
    def parse(self, request):
        "Assumption: Assuming we should get match id first"
        match = Matches()
        match.get_scorecard()


def parser_factory(event: list) -> Parser:
    if len(event) <= 5:
        return InfoEntryParser()
    else:
        return ScoreEntryParser()


"Method to handle events from webhook calls or websocket"
def handle_stream_message(event):
    parser = parser_factory(event)
    resp = parser.parse(event)

    # Send to FE response
    print(resp)



if __name__ == "__main__":
    with open('../ipl_csv2/335983_info.csv') as f:
        dict = csv.reader(f)
        for row in dict:
            handle_stream_message(row)