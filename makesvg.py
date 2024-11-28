from __future__ import annotations
import dis

import apsw, pprint, sys, apsw.ext, math, apsw.shell


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
    return f"""<path id="{id}" d="{ds}" stroke="black" fill="{fill}" stroke-width="1"/>"""

def text(id: str, usage)->str:
    return f"""<text id="{id}">{usage}</text>"""


# controls how much whitespace is around the edges
OVERSCAN = 1.2
header = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="-{round(RADIUS*OVERSCAN)} {-round(RADIUS*OVERSCAN)} {round(RADIUS*OVERSCAN*2)} {round(RADIUS*OVERSCAN*2)}">"""
footer = "</svg>"

# maps which element hovering over causes a response on
hover_response :dict[str,str]= {}

id_counter = 0
def next_id():
    global id_counter
    id_counter+=1
    return f"id{id_counter}"

out = [header]

# inner summary circle
id = next_id()
out.append(f"""<circle id="{id}" r="{c(.3)}" fill="#777"/>""")

resp=next_id()
out.append(text(resp, root))
hover_response[id]=resp



ring1_start = 0
for name, usage in group.items():
    ring1_proportion = usage.pages_used / root.pages_total
    id=next_id()
    out.append(slice(id, ring1_start, ring1_start + ring1_proportion, 1 / 3, 0.6))
    ring2_start = ring1_start
    ring1_start += ring1_proportion

    for child in sorted(usage.tables + usage.indices):
        usage2 = each[child]
        ring2_proportion = usage2.pages_used / root.pages_total
        id=next_id()
        out.append(slice(id, ring2_start, ring2_start + ring2_proportion, 2 / 3, 1.0))
        ring2_start += ring2_proportion

out.append("<style>")
for source, target in hover_response.items():
    out.append(f"""#{target} {{ display: none;}}""")
    out.append(f"""#{source}:hover ~ #{target}, #{target}:hover {{display:block;}}""")
out.append("</style>")

with open(sys.argv[2], "wt") as f:
    f.write("\n".join(out))
