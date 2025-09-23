def stab(move_type, pokemon_types):
    return 1.2 if move_type in pokemon_types else 1.0

def dps(base_power, duration_s, eff=1.0, stab_mult=1.0, weather=1.0):
    if duration_s <= 0: duration_s = 1.0
    return base_power * eff * stab_mult * weather / duration_s
