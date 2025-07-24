from typing import Dict, List
TEMPLATES: Dict[str, str] = {
    "dmg_to_champs": (
        "{nickname}, потрясающе! В матче {matchId} на {champion} ты нанёс {value} "
        "урона по чемпионам — твой лучший результат за последнюю неделю."
    ),
    "dmg_total": (
        "{nickname}, настоящая армия урона! {value} суммарного урона в матче "
        "{matchId} на {champion} — выше всех твоих игр за неделю."
    ),
    "dmg_turrets": (
        "{nickname}, башни дрожали! В матче {matchId} на {champion} ты нанёс {value} "
        "урона по башням — новый максимум недели."
    ),
    "dmg_objectives": (
        "{nickname}, охотник на цели! {value} урона по объективам в матче {matchId} "
        "на {champion} — лучший показатель за неделю."
    ),
    "gold_earned": (
        "{nickname}, золотая жила! {value} золота в матче {matchId} на "
        "{champion} — больше, чем в любой игре за неделю."
    ),
    "kills": (
        "{nickname}, машина убийств! {value} киллов в матче {matchId} на {champion} "
        "— рекорд недели."
    ),
    "assists": (
        "{nickname}, герой поддержки! {value} ассистов в матче {matchId} на {champion} "
        "— лучший командный вклад за неделю."
    ),
    "cs": (
        "{nickname}, фермер‑профи! {value} добитых миньонов в матче {matchId} на "
        "{champion} — лучший фарм недели."
    ),
    "jungle_kills": (
        "{nickname}, властелин нейтралов! {value} баронов и драконов в матче {matchId} "
        "на {champion} — новый максимум недели."
    ),
    "turret_kills": (
        "{nickname}, разрушитель! Ты снёс {value} башен в матче {matchId} на {champion} "
        "— лучший результат недели."
    ),
    "inhib_kills": (
        "{nickname}, ломатель ингибов! {value} ингибиторов в матче {matchId} на {champion} "
        "— рекорд недели."
    ),
    "pinks": (
        "{nickname}, властелин вижна! {value} контроль‑вардов куплено в матче {matchId} "
        "на {champion} — максимум недели."
    ),
    "vision_score": (
        "{nickname}, светоч карты! Vision Score {value} в матче {matchId} на {champion} "
        "— выше, чем когда‑либо за неделю."
    ),
    "cc_time": (
        "{nickname}, мастер контроля! {value} секунд CC в матче {matchId} на {champion} "
        "— лучший показатель недели."
    ),
    "dmg_mitigated": (
        "{nickname}, железная стена! Ты поглотил {value} урона в матче {matchId} на "
        "{champion} — новый личный максимум за неделю."
    ),
    "first_blood_kill": (
        "{nickname}, стремительный старт! Ты взял First Blood в матче {matchId} на "
        "{champion} — редкое достижение за неделю."
    ),
    "immortal": (
        "{nickname}, безупречно! Ты закончил матч {matchId} на {champion} без смертей "
        "— первый такой за неделю."
    ),
    "triple_kills": (
        "{nickname}, трипл‑шторм! {value} Triple Kills в матче {matchId} на {champion} "
        "— рекорд недели."
    ),
    "quadra_kills": (
        "{nickname}, квадра‑герой! {value} Quadra Kills в матче {matchId} на {champion} "
        "— лучший показатель недели."
    ),
    "penta_kills": (
        "{nickname}, PENTAKILL! Ты сделал пенту в матче {matchId} на {champion} "
        "— первая за последнюю неделю."
    ),
    "heals_team": (
        "{nickname}, целитель! Ты восстановил союзникам {value} здоровья в матче {matchId} "
        "на {champion} — рекорд недели."
    ),
    "shields_team": (
        "{nickname}, непробиваемый! Ты подарил щитов на {value} урона в матче {matchId} "
        "на {champion} — лучший вклад недели."
    ),
    "longest_life": (
        "{nickname}, феникс! Ты прожил {value} с без смерти в матче {matchId} на {champion} "
        "— самая длинная жизнь недели."
    ),
    "cspm": (
        "{nickname}, CS‑машина! Фарм {value} CS/мин в матче {matchId} на {champion} "
        "— рекорд недели."
    ),
    "interceptor": (
        "{nickname}, король стилов! {value} украденных целей в матче {matchId} на {champion} "
        "— новый максимум недели."
    ),
    "wards_killed": (
        "{nickname}, охотник на варды! Ты уничтожил {value} вардов в матче {matchId} на "
        "{champion} — лучший результат недели."
    ),
    "wards_placed": (
        "{nickname}, проводник света! Ты поставил {value} вардов в матче {matchId} на {champion} "
        "— рекорд недели."
    ),
    "dpm": (
        "{nickname}, метеорит! DPM {value} в матче {matchId} на {champion} — самый высокий за неделю."
    ),
    "gpm": (
        "{nickname}, банкир! GPM {value} в матче {matchId} на {champion} — лучший финансовый темп недели."
    ),
    "enemy_jungle": (
        "{nickname}, лесной захватчик! Ты выфармил {value} вражеских крипов в матче {matchId} на {champion} — рекорд недели."
    ),
    "neutral_kills": (
        "{nickname}, лорд нейтралов! Ты забрал {value} нейтралов в матче {matchId} на {champion} — новый максимум недели."
    ),
    "phys_dmg": (
        "{nickname}, физ‑гром! {value} физического урона в матче {matchId} на {champion} — рекорд недели."
    ),
    "magic_dmg": (
        "{nickname}, магистр арканы! {value} магического урона в матче {matchId} на {champion} — лучший результат недели."
    ),
    "true_dmg": (
        "{nickname}, истинный калибр! {value} true‑урона в матче {matchId} на {champion} — новый максимум недели."
    ),
    "dmg_taken": (
        "{nickname}, тяжёлый танк! Ты принял {value} урона в матче {matchId} на {champion} — больше, чем когда‑либо за неделю."
    ),
    "largest_crit": (
        "{nickname}, легенда критов! Крит на {value} в матче {matchId} на {champion} — самый мощный за неделю."
    ),
    "double_kills": (
        "{nickname}, двойной удар! {value} Double Kills в матче {matchId} на {champion} — рекорд недели."
    ),
    "sprees": (
        "{nickname}, снежный ком! Серия из {value} убийств в матче {matchId} на {champion} — самый длинный стрик недели."
    ),
    "gold_unspent": (
        "{nickname}, скряга! Ты закончил матч {matchId} на {champion} с {value} непр потраченного золота — необычный рекорд недели."
    ),
    "flash_casts": (
        "{nickname}, флэш‑боец! Ты использовал «Вспышку» {value} раз в матче {matchId} на {champion} — больше, чем в любой игре недели."
    ),
        "undying_ratio": (
        "{nickname}, почти бессмертен! Всего {value}% времени в смерти "
        "в матче {matchId} на {champion} — лучший показатель недели."
    ),
    "champ_level": (
        "{nickname}, мастер опыта! Ты достиг уровня {value} в матче {matchId} "
        "на {champion} — выше, чем в любых играх за неделю."
    ),
    "guard_angel": (
        "{nickname}, ангел-хранитель! Общее лечение + щиты {value} в матче "
        "{matchId} на {champion} — лучший вклад в команду за неделю."
    )
}

# Порядок метрик (для скрипта)
METRIC_COLS: List[str] = list(TEMPLATES.keys())