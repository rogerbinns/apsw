from __future__ import annotations

import apsw, pprint, sys, apsw.ext, math, apsw.shell, os, html, math
from fractions import Fraction


con = apsw.Connection(sys.argv[1])


root, group, each = (apsw.ext.analyze_pages(con, n) for n in range(3))

if False:
    pprint.pprint(root)
    pprint.pprint(group)

# angles are 0.0 through 1.0
# distance (radius) is 0 through 1.0

# use this as radius for output coordinates as int
RADIUS = 1000


def colour_for_angle(angle: float) -> str:
    # these are zero to one and cover how dark to light the colours
    # become.  base is added to each rgb, while span is how much
    # the values range above that.
    base = 0
    span = 1
    assert base + span <= 1

    radians = angle * math.pi
    third = 1 / 3 * math.pi

    red = int(255 * (base + span * abs(math.cos(radians))))
    green = int(255 * (base + span * abs(math.cos(third + radians))))
    blue = int(255 * (base + span * abs(math.cos(third + third + radians))))

    return f"#{red:02x}{green:02x}{blue:02x}"


def pos_for_angle(angle: float, distance: float) -> tuple[float, float]:
    "give x,y for distance from centre"

    # the minus bit is because trig has east as 0 but we want north as
    # zero
    radians = angle * 2 * math.pi - (1 / 4 * 2 * math.pi)

    return distance * math.cos(radians), distance * math.sin(radians)


def c(v: float | list[float]) -> str:
    # outputs a coordinate scaling by RADIUS
    if isinstance(v, float):
        return str(round(v * RADIUS))
    return " ".join(str(round(x * RADIUS)) for x in v)


def p(angle, distance):
    return c(pos_for_angle(angle, distance))


def slice(id: str, start_angle: float, end_angle: float, start_distance: float, end_distance: float):
    assert 0 <= start_angle <= 1.0
    assert 0 <= end_angle <= 1.0
    assert end_angle > start_angle
    assert 0 <= start_distance <= 1.0
    assert 0 <= end_distance <= 1.0
    assert end_distance > start_distance

    l = 1 if (end_angle - start_angle) > 1 / 2 else 0

    d = []
    d.append(f"M {p(start_angle, start_distance)}")
    d.append(f"L {p(start_angle, end_distance)}")
    d.append(f"A {c(end_distance)} {c(end_distance)} 0 {l} 1 {p(end_angle, end_distance)}")
    d.append(f"L {p(end_angle, start_distance)}")
    d.append(f"A {c(start_distance)} {c(start_distance)} 0 {l} 0 {p(start_angle, start_distance)}")

    ds = " ".join(d)

    fill = colour_for_angle((start_angle + end_angle) / 2)
    return f"""<a href="#" id="{id}"><path d="{ds}" stroke="black" fill="{fill}" stroke-width="1px"/></a>"""


def text(pos: tuple[float, float], id: str, name: str, ring: int, usage) -> str:
    x, y = c(pos[0]), c(pos[1])
    e = html.escape
    res = f"""<text id="{id}" x="{x}" y="{y}" class="infobox">"""
    res += f"""<tspan x="{x}" dy="-2em" class="name">{e(name)}</tspan>"""
    if ring == 0:
        assert isinstance(usage, apsw.ext.DatabasePageUsage)
        total = storage(usage.pages_total * usage.page_size)
        used = storage(usage.pages_used * usage.page_size)
        res += f"""<tspan x="{x}" dy="1em">{used} / {total}</tspan>"""
        res += f"""<tspan x="{x}" dy="1em">{len(usage.tables):,} tables</tspan>"""
        res += f"""<tspan x="{x}" dy="1em">{len(usage.indices):,} indices</tspan>"""
    else:
        if ring == 2:
            kind = "table" if usage.tables else "index"
            res += f"""<tspan x="{x}" dy="1em">({kind})</tspan>"""
        size = storage(usage.pages_used * usage.page_size)
        res += f"""<tspan x="{x}" dy="1em">{size}</tspan>"""
        if ring == 1:
            res += f"""<tspan x="{x}" dy="1em">{len(usage.tables):,} tables</tspan>"""
            res += f"""<tspan x="{x}" dy="1em">{len(usage.indices):,} indices</tspan>"""
    res += """</text>"""
    return res


def storage(v):
    if not v:
        return "0"
    power = math.floor(math.log(v, 1024))
    suffix = ["B", "KB", "MB", "GB", "TB", "PB", "EB"][int(power)]
    if suffix == "B":
        return f"{v}B"
    return f"{v / 1024**power:.1f}".rstrip(".0") + suffix


# controls how much whitespace is around the edges
OVERSCAN = 1.05
header = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="-{round(RADIUS*OVERSCAN)} {-round(RADIUS*OVERSCAN)} {round(RADIUS*OVERSCAN*2)} {round(RADIUS*OVERSCAN*2)}">"""
footer = "</svg>"

# maps which element hovering over causes a response on
hover_response: dict[str, str] = {}

id_counter = 0


def next_id():
    global id_counter
    id_counter += 1
    return f"id{id_counter}"


out = [header]
# z-order is based on output order so texts go last
texts: list[str] = []

# inner summary circle
id = next_id()
out.append(f"""<a href="#" id="{id}"><circle r="{c(.3)}" fill="#777"/></a>""")

resp = next_id()
texts.append(text(pos_for_angle(0, 0), resp, os.path.basename(sys.argv[1]), 0, root))
hover_response[id] = resp


start = Fraction()
for name, usage in group.items():
    ring1_proportion = Fraction(usage.pages_used, root.pages_total)
    id = next_id()
    out.append(slice(id, float(start), float(start + ring1_proportion), 1 / 3, 0.6))
    resp = next_id()
    texts.append(text(pos_for_angle(float(start + ring1_proportion / 2), (1 / 3 + 0.6) / 2), resp, name, 1, usage))
    hover_response[id] = resp
    ring2_start = start
    start += ring1_proportion

    for child in sorted(usage.tables + usage.indices):
        usage2 = each[child]
        ring2_proportion = Fraction(usage2.pages_used, root.pages_total)
        id = next_id()
        out.append(slice(id, float(ring2_start), float(ring2_start + ring2_proportion), 2 / 3, 1.0))
        resp = next_id()
        texts.append(
            text(pos_for_angle(float(ring2_start + ring2_proportion / 2), (2 / 3 + 1) / 2), resp, child, 2, usage2)
        )
        hover_response[id] = resp
        ring2_start += ring2_proportion

out.extend(texts)

out.append("<style>")
out.append(""".infobox { text-anchor: middle; dominant-baseline: middle; font-size: 20pt;
           fill: black; stroke: white; stroke-width:4pt; paint-order: stroke;
           font-family: sans-serif; }
.name {font-weight: bold;}""")
for source, target in hover_response.items():
    out.append(f"""#{target} {{ display: none;}}""")
    out.append(f"""#{source}:hover ~ #{target}, #{target}:hover, #{source}:active ~ #{target}, #{target}:active
               {{display:block;}}""")
out.append("</style>")

out.append(footer)
with open(sys.argv[2], "wt") as f:
    f.write("\n".join(out))
