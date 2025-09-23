TYPES = ["Normal","Fire","Water","Electric","Grass","Ice","Fighting","Poison","Ground","Flying",
         "Psychic","Bug","Rock","Ghost","Dragon","Dark","Steel","Fairy"]

EFF = {t:{u:1.0 for u in TYPES} for t in TYPES}
def s(a,b,val): EFF[a][b]=val

for g in ["Rock","Steel","Fire"]: s("Water", g, 1.6)
s("Fighting","Steel",1.6); s("Fighting","Rock",1.6); s("Fighting","Dark",1.6); s("Fighting","Ice",1.6); s("Fighting","Normal",1.6)
s("Ground","Steel",1.6); s("Ground","Rock",1.6); s("Ground","Fire",1.6); s("Ground","Electric",1.6)
s("Fire","Steel",1.6); s("Fire","Grass",1.6); s("Fire","Ice",1.6); s("Fire","Bug",1.6)
s("Steel","Rock",1.6); s("Steel","Ice",1.6); s("Steel","Fairy",1.6)
s("Electric","Water",1.6); s("Electric","Flying",1.6)
s("Ghost","Ghost",1.6); s("Ghost","Psychic",1.6)
s("Dark","Psychic",1.6); s("Dark","Ghost",1.6)
s("Fairy","Dragon",1.6); s("Dragon","Dragon",1.6)

for g in ["Fire","Water","Electric","Steel"]: s("Grass", g, 0.625)
s("Dragon","Steel",0.625); s("Poison","Steel",0.625); s("Normal","Ghost",0.39); s("Ghost","Normal",0.39)
