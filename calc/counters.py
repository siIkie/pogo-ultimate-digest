from .type_effectiveness import EFF, TYPES

def best_attacking_types(defending_types):
    scores = {t:1.0 for t in TYPES}
    for atk in TYPES:
        mult = 1.0
        for d in defending_types:
            mult *= EFF[atk].get(d,1.0)
        scores[atk] = mult
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)

def suggest_counters(defending_types, topn=5):
    ranked = best_attacking_types(defending_types)
    return [t for t,score in ranked[:topn] if score>1.0]
