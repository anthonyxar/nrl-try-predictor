NRL_TEAMS = {
    "broncos": {"name": "Brisbane Broncos", "short": "BRI", "color": "#6D2735"},
    "raiders": {"name": "Canberra Raiders", "short": "CAN", "color": "#56B947"},
    "bulldogs": {"name": "Canterbury Bulldogs", "short": "CBY", "color": "#003B7B"},
    "sharks": {"name": "Cronulla Sharks", "short": "CRO", "color": "#00A5DB"},
    "titans": {"name": "Gold Coast Titans", "short": "GLD", "color": "#E8B825"},
    "sea_eagles": {"name": "Manly Sea Eagles", "short": "MAN", "color": "#6D2735"},
    "storm": {"name": "Melbourne Storm", "short": "MEL", "color": "#552D6D"},
    "knights": {"name": "Newcastle Knights", "short": "NEW", "color": "#003B7B"},
    "cowboys": {"name": "North Queensland Cowboys", "short": "NQL", "color": "#002B5C"},
    "eels": {"name": "Parramatta Eels", "short": "PAR", "color": "#005DB5"},
    "panthers": {"name": "Penrith Panthers", "short": "PEN", "color": "#2A2A2A"},
    "rabbitohs": {"name": "South Sydney Rabbitohs", "short": "SOU", "color": "#003B2F"},
    "dragons": {"name": "St George Illawarra Dragons", "short": "SGI", "color": "#E2231A"},
    "roosters": {"name": "Sydney Roosters", "short": "SYD", "color": "#003B7B"},
    "warriors": {"name": "New Zealand Warriors", "short": "NZL", "color": "#636466"},
    "tigers": {"name": "Wests Tigers", "short": "WST", "color": "#F47920"},
    "dolphins": {"name": "Dolphins", "short": "DOL", "color": "#C8102E"},
}

POSITIONS = {
    1: "Fullback",
    2: "Wing",
    3: "Centre",
    4: "Centre",
    5: "Wing",
    6: "Five-Eighth",
    7: "Halfback",
    8: "Prop",
    9: "Hooker",
    10: "Prop",
    11: "Second Row",
    12: "Second Row",
    13: "Lock",
    14: "Interchange",
    15: "Interchange",
    16: "Interchange",
    17: "Interchange",
}

# Base try-scoring rates by position (tries per game historically)
POSITION_TRY_RATES = {
    1: 0.28,   # Fullback - high involvement, kick returns
    2: 0.35,   # Wing - primary try scorer
    3: 0.22,   # Centre
    4: 0.22,   # Centre
    5: 0.35,   # Wing
    6: 0.15,   # Five-Eighth
    7: 0.12,   # Halfback
    8: 0.06,   # Prop
    9: 0.10,   # Hooker
    10: 0.06,  # Prop
    11: 0.10,  # Second Row
    12: 0.10,  # Second Row
    13: 0.08,  # Lock
    14: 0.06,  # Interchange
    15: 0.04,  # Interchange
    16: 0.04,  # Interchange
    17: 0.04,  # Interchange
}
