from Transformation.Pattern import Pattern
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock


def parse_transformations(path, min_support=0.0):
    all_txt_tr = []

    with open(path, 'r') as f:
        init_pt = "pattern list: {"
        init_aj = "ranked list: {"

        line = ""
        while not (line.startswith(init_pt) or line.startswith(init_aj)):
            line = f.readline()

        line = f.readline().strip()

        while line != "" and line != "****Golden rows apply patterns******" and line != "}":
            assert line.startswith("{[")
            ll = line.strip().split("  -- ")[0].split(" -> ")
            assert len(ll) == 2
            tr = ll[0]

            line = f.readline().strip()
            while line.startswith('|->'):
                line = f.readline().strip()

            all_txt_tr.append({
                'trans': tr,
                'coverage': int(ll[1].split("/")[0]) / int(ll[1].strip().split("/")[1])
            })

    final_trans = []
    for item in all_txt_tr:

        if item['coverage'] < min_support:
            continue

        tr = item['trans']
        assert tr[0] == '{' and tr[-1] == '}'
        tr = tr[1:-1] # remove first and last { }
        tr = ", " + tr.strip() + " ["

        # assert tr.count(", [") == tr.count("], ")
        blocks = tr.split("], [")

        assert blocks[-1] == ""
        blocks = blocks[:-1]
        assert blocks[0].startswith(", [")
        blocks[0] = blocks[0][3:]

        # print(tr[2:-3], end='--->')
        blks = []
        for b in blocks:

            ttype = b.split(":")[0]

            if ttype == "LIT":
                bk = LiteralPatternBlock(b[5:-1])
                blks.append(bk)

            elif ttype == "POS" or ttype == "Substr":
                tmp = b.split(":")
                assert len(tmp) == 2  # tmp[1] ==> e.g.: (7-9)
                assert tmp[1][0] == "(" and tmp[1][-1] == ")"
                tmp[1] = tmp[1][1:-1]  # remove ( and )
                nums = tmp[1].split("-")
                assert len(nums) == 2
                start = int(nums[0])
                end = int(nums[1])
                bk = PositionPatternBlock(start, end)
                blks.append(bk)

            elif ttype == "TOK" or ttype == "Split":
                index = None
                txt = None
                if b.strip()[-1] == "]": # multi-index split
                    assert b.count("', [") == 1
                    tmp = b.split("', [")[1].strip()
                    assert tmp[-1] == "]"
                    tmp = tmp[:-1]
                    index = [int(m.strip()) for m in tmp.split(",")]
                    txt = b[len(ttype)+3:b.index("', [")]
                else:
                    index = int(b.split(",")[-1].strip())
                    txt = b[len(ttype)+3:b.rindex("',")]
                bk = TokenPatternBlock(txt, index)
                blks.append(bk)
            elif ttype == "SplitSubstr":
                tmp = b.split(",")
                substr = tmp[-1].strip()
                assert substr[0] == "(" and substr[-1] == ")"
                substr = substr[1:-1]  # remove ( and )
                nums = substr.split("-")
                assert len(nums) == 2
                start = int(nums[0])
                end = int(nums[1])
                index = int(tmp[-2].strip())

                tmp = b.split(",")
                x = len(b) - len(tmp[-1]) - len(tmp[-2]) - 3
                txt = b[len(ttype)+3:x]

                bk = SplitSubstrPatternBlock(txt, index, start, end)
                blks.append(bk)
            else:
                raise Exception("Transformation unit type \""+ttype+"\" is unknown")

        trans = Pattern(blks)
        # print(trans)
        final_trans.append(trans)

    return final_trans

