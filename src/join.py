import pathlib

from data_processor import DataLoader as dl
from pattern import joiner
from evaluator.JoinEval import JoinEval
from data_processor import Matcher as matcher
from pattern import Finder
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock

params = {
    'max_tokens': 3,  # maximum number of allowed placeholders
    'max_blocks': 3,  # maximum number of allowed blocks (either placeholder or literal)
    'generalize': False,
    'sample_size': None,

    'token_splitters': [' ', ],
    # set to None to disable. Break placeholders into other placeholders based on these chars
    'remove_duplicate_patterns': True,
    # After generating all possible transformation, delete the duplicates, may take time
    'switch_literals_placeholders': True,  # Replace placeholder with literals and add them as new pattern
    'only_first_match': False,  # Take only first match for the placeholder or look for all of possible matches.

    'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock],
    # literal must be included
}


'''
An example of end2end join
'''


def main():
    base_path = str(pathlib.Path(__file__).absolute().parent.parent.absolute())
    db_path = base_path + '/data/autojoin-Benchmark/fsu name to username'

    # Load source, target, and ground truth columns:
    src, target, gt = dl.basic_loader(db_path, make_lower=True)

    # Call the join function
    join(src, target, gt)


def join(src, target, gt):
    # Get matching rows:
    rows, is_swapped = matcher.get_matching_rows(src, target, q_start=4, q_end=20, swap_src_target=True)

    # Get transformations:
    trs = Finder.get_patterns(rows, params=params, table_name='test tbl', verbose=False)
    trans = [t[2] for t in trs['ranked']]

    # Perform the join:
    joins = joiner.join(src, target, trans, is_swapped)

    # Evaluate the join:
    je = JoinEval(joins, gt)
    print(je)


if __name__ == "__main__":
    main()



