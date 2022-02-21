import argparse
import os
import pathlib
import shutil
import random

from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Pattern import Pattern
from pattern import Unifier

ADD_SPACE_BETWEEN_TOKENS = False

BASE_PATH = str(pathlib.Path(__file__).absolute().parent.parent.absolute())
DS_PATH = BASE_PATH + '/data/synthetic/'

LETTERS = 'abcdefghijklmnopqrstuvwxyz1234567890'
INPUT_CHAR_SET = LETTERS # + '  '  # more chance for space

PT_BLOCKS = [PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock] + [PositionPatternBlock, TokenPatternBlock]
# PT_BLOCKS = [PositionPatternBlock, TokenPatternBlock]


def generate_synthetic_dataset(args, dir_name):
    num_row = args['rows']
    dir_path = DS_PATH + dir_name + '/'

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.mkdir(dir_path)

    sum_tr_len, sum_literals, sum_blocks, sum_avg_lit_len = 0, 0, 0, 0
    num_tr = args['transformations']
    tr_list = []
    tr_coverage = []
    for i in range(num_tr):
        num_blocks = random.randint(args['blocks'][0], args['blocks'][1])
        num_literals = random.randint(args['literals'][0], args['literals'][1])
        sum_blocks += num_blocks
        sum_literals += num_literals
        tr_len = num_blocks + num_literals
        sum_tr_len += tr_len

        literals = []
        sum_ll = 0
        for j in range(num_literals):
            lit_len = random.randint(args['literal_len'][0], args['literal_len'][1])
            sum_ll += lit_len
            literals.append(LiteralPatternBlock(''.join(random.choice(INPUT_CHAR_SET) for i in range(lit_len))))
        sum_avg_lit_len += sum_ll / num_literals

        trs = []
        for j in range(num_blocks):
            cls = random.choice(PT_BLOCKS)
            trs.append(cls.get_random(INPUT_CHAR_SET, args['input_len'][1]//3))

        blks = literals + trs
        random.shuffle(blks)
        pt = Unifier.merge_literals(Pattern(blks))

        if ADD_SPACE_BETWEEN_TOKENS:
            blks = []
            txt = ""
            for bl in pt.blocks:
                if type(bl) == LiteralPatternBlock:
                    txt += bl.text
                else:
                    if txt != " ":
                        txt += " "
                    blks.append(LiteralPatternBlock(txt))
                    txt = " "
                    blks.append(bl)
            if txt != "" and txt != " ":
                blks.append(LiteralPatternBlock(txt))

            if type(blks[0]) == LiteralPatternBlock and blks[0].text == " ":
                blks = blks[1:]

            pt = Pattern(blks)
        ###########

        tr_list.append(pt)
        tr_coverage.append(0)





    with open(dir_path + 'rows.txt', 'w') as f:
        print(f"{args['src_col_name']}:{args['target_col_name']}", file=f)
        print("source", file=f)

    src_file = open(dir_path + 'source.csv', 'w')
    target_file = open(dir_path + 'target.csv', 'w')
    gt_file = open(dir_path + 'ground truth.csv', 'w')
    tr_file = open(dir_path + 'transformations.csv', 'w')

    print(f"{args['src_col_name']}", file=src_file)
    print(f"{args['target_col_name']}", file=target_file)
    print(f"source-{args['src_col_name']},target-{args['target_col_name']}", file=gt_file)
    print(f"source-{args['src_col_name']},trans,target-{args['target_col_name']}", file=tr_file)

    cnt = 0
    failed_tries = 0
    sum_len = 0

    for i in range(0, num_row):
        if cnt % 100 == 0:
            print(f"batch {cnt}/{num_row}")
        cnt += 1

        inp_len = random.randint(args['input_len'][0], args['input_len'][1])

        sum_len += inp_len


        tr_idx = random.randrange(0, len(tr_list))
        tr = tr_list[tr_idx]
        tr_coverage[tr_idx] += 1

        src = random.choice(LETTERS) + ''.join(random.choice(INPUT_CHAR_SET) for i in range(inp_len-2)) + random.choice(LETTERS)
        target = tr.apply(src)

        while target is None:
            failed_tries += 1
            src = random.choice(LETTERS) + ''.join(random.choice(INPUT_CHAR_SET) for i in range(inp_len - 2)) + random.choice(LETTERS)
            target = tr.apply(src)

        print(src, file=src_file)
        print(target, file=target_file)
        print(f"{src},{target}", file=gt_file)
        print(f"{src},{str(tr).replace(',', '')},{target}", file=tr_file)

    src_file.close()
    target_file.close()
    gt_file.close()
    tr_file.close()

    with open(dir_path + "ds_info.txt", 'w') as f:
        print(f"Params: {args}", file=f)
        print(f"Number of rows: {num_row}", file=f)
        print(f"Avg Input Len: {sum_len/num_row}", file=f)
        print(f"Number of transformations: {num_tr}", file=f)
        print(f"Avg blocks per transformation (trans. len)(before merging literals): {sum_tr_len/num_tr}", file=f)
        print(f"Avg Placeholders(units) per transformation (before merging literals): {sum_blocks/num_tr}", file=f)
        print(f"Avg literal per transformation (before merging literals): {sum_literals/num_tr}", file=f)
        print(f"  Mean Avg literal length (before merging literals): {sum_avg_lit_len/num_tr}", file=f)
        print(f"Transformation:\n    "+'{', file=f)
        assert len(tr_list) == len(tr_coverage)
        for tr, c in zip(tr_list, tr_coverage):
            print(f"      {tr} --> {c}", file=f)
        print(f"    "+'}', file=f)
        print(f"blocks: {[str(b.__name__) for b in PT_BLOCKS]}", file=f)
        print(f"Input char set: {INPUT_CHAR_SET}", file=f)
        print(f"Failed output generate tries:: {failed_tries}", file=f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', '-r', action='store', type=int, required=False,
                        default=500, help='Number of rows for Dataset')

    parser.add_argument('--input_len', '-il', action='store', type=int, nargs=2, metavar=('min', 'max'), required=False,
                        default=[20, 35], help='Input length')

    parser.add_argument('--transformations', '-t', action='store', type=int, required=False,
                        default=3, help='Number of transformations to cover the dataset')

    parser.add_argument('--blocks', '-b', action='store', type=int, nargs=2, metavar=('min', 'max'), required=False,
                        default=[2, 3], help='Number of placeholders per transformation (Parts of output that is generated from input)')

    parser.add_argument('--literals', '-l', action='store', type=int, nargs=2, metavar=('min', 'max'), required=False,
                        default=[1, 2], help='Number of literal blocks per transformation')

    parser.add_argument('--literal_len', '-ll', action='store', type=int, nargs=2, metavar=('min', 'max'), required=False,
                        default=[1, 4], help='Length of literal blocks')




    # Less important ones
    parser.add_argument('--src_col_name', action='store', type=str, required=False, default="STitle",
                        help='Title for key columns in source table')
    parser.add_argument('--target_col_name', action='store', type=str, required=False, default="TTitle",
                        help='Title for key columns in target table')


    args = parser.parse_args().__dict__

    generate_synthetic_dataset(args, f"synthetic-{args['rows']:04}rows")

    # for i in range(10, 501, 10):
    #     print(f"Len: {i}")
    #     args['input_len'] = [i, i]
    #     generate_synthetic_dataset(args, f"synthetic-inp-len{i:03}")


if __name__ == '__main__':
    main()
