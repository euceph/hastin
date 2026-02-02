from hastin.DataTypes import ConnectionSource


class CommandManager:
    def __init__(self):
        self.command_keys = {
            ConnectionSource.postgresql: {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {
                        "human_key": "3",
                        "description": "Toggle panel Metric Graphs",
                    },
                    "4": {
                        "human_key": "4",
                        "description": "Toggle panel Replication",
                    },
                    "5": {
                        "human_key": "5",
                        "description": "Toggle panel Locks",
                    },
                    "6": {
                        "human_key": "6",
                        "description": "Toggle panel Statements (pg_stat_statements)",
                    },
                    "7": {
                        "human_key": "7",
                        "description": "Toggle panel PgBouncer (requires --pgbouncer-host)",
                    },
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {
                        "human_key": "-",
                        "description": "Remove the current tab",
                    },
                    "equals_sign": {
                        "human_key": "=",
                        "description": "Rename the current tab",
                    },
                    "D": {
                        "human_key": "D",
                        "description": "Disconnect from the tab's host",
                    },
                    "ctrl+a": {
                        "human_key": "ctrl+a",
                        "description": "Switch to the previous tab",
                    },
                    "ctrl+d": {
                        "human_key": "ctrl+d",
                        "description": "Switch to the next tab",
                    },
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {
                        "human_key": "a",
                        "description": "Toggle additional processlist columns",
                    },
                    "i": {
                        "human_key": "i",
                        "description": "Toggle display of idle threads",
                    },
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads with active transactions",
                    },
                    "p": {
                        "human_key": "p",
                        "description": "Toggle pause for refreshing of panels",
                    },
                    "s": {
                        "human_key": "s",
                        "description": "Toggle sorting for Age in Processlist panel",
                    },
                    "S": {
                        "human_key": "S",
                        "description": "Toggle Statistics/s section in Dashboard",
                    },
                    "placeholder_3": {"human_key": "", "description": ""},
                    "l": {
                        "human_key": "l",
                        "description": "Display blocked queries / lock waits",
                    },
                    "d": {"human_key": "d", "description": "Display all databases"},
                    "t": {
                        "human_key": "t",
                        "description": "Display details of a thread along with EXPLAIN",
                    },
                    "u": {
                        "human_key": "u",
                        "description": "Display connected users and their statistics",
                    },
                    "v": {
                        "human_key": "v",
                        "description": "Display PostgreSQL settings (pg_settings)",
                    },
                    "z": {
                        "human_key": "z",
                        "description": "Display all entries in the host cache",
                    },
                    "Z": {
                        "human_key": "Z",
                        "description": "Display table sizes",
                    },
                    "placeholder_4": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {
                        "human_key": "f",
                        "description": "Filter threads by field(s)",
                    },
                    "E": {
                        "human_key": "E",
                        "description": "Export the processlist to a CSV file",
                    },
                    "k": {"human_key": "k", "description": "Kill backend(s)"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                    "R": {"human_key": "R", "description": "Reset all metrics"},
                    "space": {
                        "human_key": "space",
                        "description": "Force a manual refresh of all panels",
                    },
                }
            },
            ConnectionSource.rds: {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {
                        "human_key": "3",
                        "description": "Toggle panel Metric Graphs",
                    },
                    "4": {
                        "human_key": "4",
                        "description": "Toggle panel Replication",
                    },
                    "5": {
                        "human_key": "5",
                        "description": "Toggle panel Locks",
                    },
                    "6": {
                        "human_key": "6",
                        "description": "Toggle panel Statements (pg_stat_statements)",
                    },
                    "7": {
                        "human_key": "7",
                        "description": "Toggle panel PgBouncer (requires --pgbouncer-host)",
                    },
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {
                        "human_key": "-",
                        "description": "Remove the current tab",
                    },
                    "equals_sign": {
                        "human_key": "=",
                        "description": "Rename the current tab",
                    },
                    "D": {
                        "human_key": "D",
                        "description": "Disconnect from the tab's host",
                    },
                    "ctrl+a": {
                        "human_key": "ctrl+a",
                        "description": "Switch to the previous tab",
                    },
                    "ctrl+d": {
                        "human_key": "ctrl+d",
                        "description": "Switch to the next tab",
                    },
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {
                        "human_key": "a",
                        "description": "Toggle additional processlist columns",
                    },
                    "i": {
                        "human_key": "i",
                        "description": "Toggle display of idle threads",
                    },
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads with active transactions",
                    },
                    "p": {
                        "human_key": "p",
                        "description": "Toggle pause for refreshing of panels",
                    },
                    "s": {
                        "human_key": "s",
                        "description": "Toggle sorting for Age in Processlist panel",
                    },
                    "S": {
                        "human_key": "S",
                        "description": "Toggle Statistics/s section in Dashboard",
                    },
                    "placeholder_3": {"human_key": "", "description": ""},
                    "l": {
                        "human_key": "l",
                        "description": "Display blocked queries / lock waits",
                    },
                    "d": {"human_key": "d", "description": "Display all databases"},
                    "t": {
                        "human_key": "t",
                        "description": "Display details of a thread along with EXPLAIN",
                    },
                    "u": {
                        "human_key": "u",
                        "description": "Display connected users and their statistics",
                    },
                    "v": {
                        "human_key": "v",
                        "description": "Display PostgreSQL settings (pg_settings)",
                    },
                    "z": {
                        "human_key": "z",
                        "description": "Display all entries in the host cache",
                    },
                    "Z": {
                        "human_key": "Z",
                        "description": "Display table sizes",
                    },
                    "placeholder_4": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {
                        "human_key": "f",
                        "description": "Filter threads by field(s)",
                    },
                    "E": {
                        "human_key": "E",
                        "description": "Export the processlist to a CSV file",
                    },
                    "k": {"human_key": "k", "description": "Kill backend(s)"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                    "R": {"human_key": "R", "description": "Reset all metrics"},
                    "space": {
                        "human_key": "space",
                        "description": "Force a manual refresh of all panels",
                    },
                }
            },
            ConnectionSource.aurora: {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {
                        "human_key": "3",
                        "description": "Toggle panel Metric Graphs",
                    },
                    "4": {
                        "human_key": "4",
                        "description": "Toggle panel Replication",
                    },
                    "5": {
                        "human_key": "5",
                        "description": "Toggle panel Locks",
                    },
                    "6": {
                        "human_key": "6",
                        "description": "Toggle panel Statements (pg_stat_statements)",
                    },
                    "7": {
                        "human_key": "7",
                        "description": "Toggle panel PgBouncer (requires --pgbouncer-host)",
                    },
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {
                        "human_key": "-",
                        "description": "Remove the current tab",
                    },
                    "equals_sign": {
                        "human_key": "=",
                        "description": "Rename the current tab",
                    },
                    "D": {
                        "human_key": "D",
                        "description": "Disconnect from the tab's host",
                    },
                    "ctrl+a": {
                        "human_key": "ctrl+a",
                        "description": "Switch to the previous tab",
                    },
                    "ctrl+d": {
                        "human_key": "ctrl+d",
                        "description": "Switch to the next tab",
                    },
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {
                        "human_key": "a",
                        "description": "Toggle additional processlist columns",
                    },
                    "i": {
                        "human_key": "i",
                        "description": "Toggle display of idle threads",
                    },
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads with active transactions",
                    },
                    "p": {
                        "human_key": "p",
                        "description": "Toggle pause for refreshing of panels",
                    },
                    "s": {
                        "human_key": "s",
                        "description": "Toggle sorting for Age in Processlist panel",
                    },
                    "S": {
                        "human_key": "S",
                        "description": "Toggle Statistics/s section in Dashboard",
                    },
                    "placeholder_3": {"human_key": "", "description": ""},
                    "l": {
                        "human_key": "l",
                        "description": "Display blocked queries / lock waits",
                    },
                    "d": {"human_key": "d", "description": "Display all databases"},
                    "t": {
                        "human_key": "t",
                        "description": "Display details of a thread along with EXPLAIN",
                    },
                    "u": {
                        "human_key": "u",
                        "description": "Display connected users and their statistics",
                    },
                    "v": {
                        "human_key": "v",
                        "description": "Display PostgreSQL settings (pg_settings)",
                    },
                    "z": {
                        "human_key": "z",
                        "description": "Display all entries in the host cache",
                    },
                    "Z": {
                        "human_key": "Z",
                        "description": "Display table sizes",
                    },
                    "placeholder_4": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {
                        "human_key": "f",
                        "description": "Filter threads by field(s)",
                    },
                    "E": {
                        "human_key": "E",
                        "description": "Export the processlist to a CSV file",
                    },
                    "k": {"human_key": "k", "description": "Kill backend(s)"},
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                    "R": {"human_key": "R", "description": "Reset all metrics"},
                    "space": {
                        "human_key": "space",
                        "description": "Force a manual refresh of all panels",
                    },
                }
            },
            "postgresql_replay": {
                "Commands": {
                    "1": {"human_key": "1", "description": "Toggle panel Dashboard"},
                    "2": {"human_key": "2", "description": "Toggle panel Processlist"},
                    "3": {
                        "human_key": "3",
                        "description": "Toggle panel Metric Graphs",
                    },
                    "4": {
                        "human_key": "4",
                        "description": "Toggle panel Replication",
                    },
                    "5": {
                        "human_key": "5",
                        "description": "Toggle panel Locks",
                    },
                    "placeholder_1": {"human_key": "", "description": ""},
                    "grave_accent": {"human_key": "`", "description": "Open tab setup"},
                    "plus": {"human_key": "+", "description": "Create a new tab"},
                    "minus": {
                        "human_key": "-",
                        "description": "Remove the current tab",
                    },
                    "equals_sign": {
                        "human_key": "=",
                        "description": "Rename the current tab",
                    },
                    "ctrl+a": {
                        "human_key": "ctrl+a",
                        "description": "Switch to the previous tab",
                    },
                    "ctrl+d": {
                        "human_key": "ctrl+d",
                        "description": "Switch to the next tab",
                    },
                    "placeholder_2": {"human_key": "", "description": ""},
                    "a": {
                        "human_key": "a",
                        "description": "Toggle additional processlist columns",
                    },
                    "T": {
                        "human_key": "T",
                        "description": "Toggle display of threads with active transactions",
                    },
                    "s": {
                        "human_key": "s",
                        "description": "Toggle sorting for Age in Processlist panel",
                    },
                    "placeholder_3": {"human_key": "", "description": ""},
                    "t": {
                        "human_key": "t",
                        "description": "Display details of a thread",
                    },
                    "v": {
                        "human_key": "v",
                        "description": "Display PostgreSQL settings",
                    },
                    "V": {
                        "human_key": "V",
                        "description": "Display settings that changed during recording",
                    },
                    "placeholder_4": {"human_key": "", "description": ""},
                    "p": {"human_key": "p", "description": "Toggle pause of replay"},
                    "S": {
                        "human_key": "S",
                        "description": "Seek to a specific time in the replay",
                    },
                    "left_square_bracket": {
                        "human_key": "[",
                        "description": "Seek to previous refresh interval in the replay",
                    },
                    "right_square_bracket": {
                        "human_key": "]",
                        "description": "Seek to next refresh interval in the replay",
                    },
                    "placeholder_5": {"human_key": "", "description": ""},
                    "c": {"human_key": "c", "description": "Clear all filters set"},
                    "f": {
                        "human_key": "f",
                        "description": "Filter threads by field(s)",
                    },
                    "E": {
                        "human_key": "E",
                        "description": "Export the processlist to a CSV file",
                    },
                    "M": {"human_key": "M", "description": "Maximize a panel"},
                    "q": {"human_key": "q", "description": "Quit"},
                    "r": {"human_key": "r", "description": "Set the refresh interval"},
                }
            },
        }

        # These are keys that we let go through no matter what
        self.exclude_keys = [
            "up",
            "down",
            "left",
            "right",
            "pageup",
            "pagedown",
            "home",
            "end",
            "tab",
            "enter",
            "grave_accent",
            "q",
            "question_mark",
            "plus",
            "minus",
            "equals_sign",
            "ctrl+a",
            "ctrl+d",
        ]

    def get_commands(self, replay_file: str, connection_source: ConnectionSource) -> dict[str, dict[str, str]]:
        key = "postgresql_replay" if replay_file else connection_source

        return self.command_keys.get(key, {}).get("Commands")
