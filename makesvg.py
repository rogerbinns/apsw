from __future__ import annotations

import apsw, pprint, sys, apsw.ext, math, apsw.shell, os, html, math
from fractions import Fraction

from typing import TextIO


def storage(v) -> str:
    """Converts number to storage size (KB, MB, GB etc)"""
    if not v:
        return "0"
    power = math.floor(math.log(v, 1024))
    suffix = ["B", "KB", "MB", "GB", "TB", "PB", "EB"][int(power)]
    if suffix == "B":
        return f"{v}B"
    return f"{v / 1024**power:.1f}".rstrip(".0") + suffix


def page_usage_to_svg(con: apsw.Connection, out: TextIO, schema: str = "main") -> None:
    """Visualize database space usage as a `SVG <https://en.wikipedia.org/wiki/SVG>`__

    You can hover or click on segments to get more details.  The
    centre circle shows information about the database as a whole, the
    middle ring shows usage grouped by database (combing indices,
    shadow tables for virtual tables), while the outer ring shows each
    index and table separately.

    Uses :func:`analyze_pages` to gather the information.

    :param con: Connection to query
    :param out: Where the svg is written to.  You can use
       :class:`io.StringIO` if you want it as a string.
    :param schema: Which attached database to query
    """
    # Angles and distances are used within.  They are in the range 0.0
    # to 1.0 .

    # Coordinates are output as int, so this is the scaling factor
    RADIUS = 1000
    # how much whitespace is outside the circles
    OVERSCAN = 1.05

    def colour_for_angle(angle: float) -> str:
        # we use r g b each offset by a third of the circle
        radians = angle * math.pi
        third = 1 / 3 * math.pi

        red = int(255 * abs(math.cos(radians)))
        green = int(255 * abs(math.cos(third + radians)))
        blue = int(255 * abs(math.cos(third + third + radians)))

        return f"#{red:02x}{green:02x}{blue:02x}"

    def pos_for_angle(angle: float, distance: float) -> tuple[float, float]:
        "give x,y for distance from centre"

        # the minus bit is because trig has east as 0 but we want north as
        # zero
        radians = angle * 2 * math.pi - (1 / 4 * 2 * math.pi)

        return distance * math.cos(radians), distance * math.sin(radians)

    # these two are used in fstrings hence the short names
    def c(v: float | list[float]) -> str:
        # outputs a coordinate scaling by RADIUS
        if isinstance(v, float):
            return str(round(v * RADIUS))
        return " ".join(str(round(x * RADIUS)) for x in v)

    def p(angle: float, distance: float):
        # outputs a coordinate scaling by RADIUS
        return c(pos_for_angle(angle, distance))

    def slice(id: str, start_angle: float, end_angle: float, start_distance: float, end_distance: float):
        # produces one of the circular slices
        large = 1 if (end_angle - start_angle) > 1 / 2 else 0

        d = []
        d.append(f"M {p(start_angle, start_distance)}")
        d.append(f"L {p(start_angle, end_distance)}")
        d.append(f"A {c(end_distance)} {c(end_distance)} 0 {large} 1 {p(end_angle, end_distance)}")
        d.append(f"L {p(end_angle, start_distance)}")
        d.append(f"A {c(start_distance)} {c(start_distance)} 0 {large} 0 {p(start_angle, start_distance)}")

        ds = " ".join(d)

        fill = colour_for_angle((start_angle + end_angle) / 2)
        return f"""<a href="#" id="{id}"><path d="{ds}" stroke="black" fill="{fill}" stroke-width="1px"/></a>"""

    def text(pos: tuple[float, float], id: str, name: str, ring: int, usage: DatabasePageUsage | PageUsage) -> str:
        # produces text infobox
        x, y = c(pos[0]), c(pos[1])
        e = html.escape
        res = []
        res.append(f"""<text id="{id}" x="{x}" y="{y}" class="infobox">""")
        res.append(f"""<tspan x="{x}" dy="-4em" class="name">{e(name)}</tspan>""")
        if ring == 0:
            assert isinstance(usage, apsw.ext.DatabasePageUsage)
            total = storage(usage.pages_total * usage.page_size)
            used = storage(usage.pages_used * usage.page_size)
            res.append(f"""<tspan x="{x}" dy="1em">{used} / {total}</tspan>""")
            res.append(f"""<tspan x="{x}" dy="1em">{len(usage.tables):,} tables</tspan>""")
            res.append(f"""<tspan x="{x}" dy="1em">{len(usage.indices):,} indices</tspan>""")
        else:
            if ring == 2:
                kind = "table" if usage.tables else "index"
                res.append(f"""<tspan x="{x}" dy="1em">({kind})</tspan>""")
            size = storage(usage.pages_used * usage.page_size)
            res.append(f"""<tspan x="{x}" dy="1em">{size}</tspan>""")
            if ring == 1:
                res.append(f"""<tspan x="{x}" dy="1em">{len(usage.tables):,} tables</tspan>""")
                res.append(f"""<tspan x="{x}" dy="1em">{len(usage.indices):,} indices</tspan>""")
        res.append(
            f"""<tspan x="{x}" dy="1em">{usage.sequential_pages/max(usage.pages_used, 1):.0%} sequential</tspan>"""
        )
        res.append(f"""<tspan x="{x}" dy="1em">{storage(usage.data_stored)} SQL data</tspan>""")
        res.append(f"""<tspan x="{x}" dy="1em">{storage(usage.max_payload)} max payload</tspan>""")
        res.append(f"""<tspan x="{x}" dy="1em">{usage.cells:,} cells</tspan>""")

        res.append("""</text>""")
        return "\n".join(res)

    # check we can get the information
    root, group, each = (apsw.ext.analyze_pages(con, n, schema) for n in range(3))

    # maps which element hovering over causes a response on
    hover_response: dict[str, str] = {}

    # z-order is based on output order so texts go last
    texts: list[str] = []

    id_counter = 0

    def next_ids():
        # return pais of element ids used to map slice to corresponding text
        nonlocal id_counter
        PREFIX = "id"
        hover_response[f"{PREFIX}{id_counter}"] = f"{PREFIX}{id_counter + 1}"
        id_counter += 2
        return f"{PREFIX}{id_counter-2}", f"{PREFIX}{id_counter-1}"

    print(
        f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="-{round(RADIUS*OVERSCAN)} {-round(RADIUS*OVERSCAN)} {round(RADIUS*OVERSCAN*2)} {round(RADIUS*OVERSCAN*2)}">""",
        file=out,
    )

    # inner summary circle
    id, resp = next_ids()
    print(f"""<a href="#" id="{id}"><circle r="{c(.3)}" fill="#777"/></a>""", file=out)
    texts.append(text(pos_for_angle(0, 0), resp, os.path.basename(sys.argv[1]), 0, root))

    # inner ring
    start = Fraction()
    for name, usage in group.items():
        ring1_proportion = Fraction(usage.pages_used, root.pages_total)
        id, resp = next_ids()
        print(slice(id, float(start), float(start + ring1_proportion), 1 / 3, 0.6), file=out)
        texts.append(text(pos_for_angle(float(start + ring1_proportion / 2), (1 / 3 + 0.6) / 2), resp, name, 1, usage))
        ring2_start = start
        start += ring1_proportion

        # corresponding outer ring
        for child in sorted(usage.tables + usage.indices):
            usage2 = each[child]
            ring2_proportion = Fraction(usage2.pages_used, root.pages_total)
            id, resp = next_ids()
            print(slice(id, float(ring2_start), float(ring2_start + ring2_proportion), 2 / 3, 1.0), file=out)
            texts.append(
                text(pos_for_angle(float(ring2_start + ring2_proportion / 2), (2 / 3 + 1) / 2), resp, child, 2, usage2)
            )
            ring2_start += ring2_proportion

    for t in texts:
        print(t, file=out)

    print(
        """<style>
        .infobox { text-anchor: middle; dominant-baseline: middle; font-size: 28pt;
            fill: black; stroke: white; stroke-width:4pt; paint-order: stroke;
            font-family: sans-serif; }
        .name {font-weight: bold;}
    """,
        file=out,
    )
    for source, target in hover_response.items():
        print(f"""#{target} {{ display: none;}}""", file=out)
        print(
            f"""#{source}:hover ~ #{target}, #{target}:hover, #{source}:active ~ #{target}, #{target}:active
                {{display:block;}}""",
            file=out,
        )

    print("</style></svg>", file=out)


con = apsw.Connection(sys.argv[1])


with open(sys.argv[2], "wt") as f:
    page_usage_to_svg(con, f)
