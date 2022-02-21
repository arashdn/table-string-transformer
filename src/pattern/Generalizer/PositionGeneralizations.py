from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock


def change_pos_split_splitsubstr(parent_idx, block_idx, inputs, patterns, pat_sim, pat_inp):
    # print("add_one_char_split")
    block = patterns[parent_idx].blocks[block_idx]
    # print(block)

    block_len = block.end - block.start
    if block_len < 6:
        return None

    assert type(block) == PositionPatternBlock
    new_splitters = set()

    # We are looking for characters that have already happened after current separator.
    # Thus, we split the input and choose first character.

    candidates = set(pat_inp[parent_idx]) # we need to cover already covered inputs, so, we only look in already matched blocks
    # candidates.update(pat_sim[parent_idx])

    for cad in candidates:
        new_splitters.update(inputs[cad][0][block.start:block.end])

    return {
        'generalization_type': 'change_pos_split_splitsubstr',
        'blocks': [TokenPatternBlock(None, None), LiteralPatternBlock(None), SplitSubstrPatternBlock(None, None, None, None)],
        'params_space': [
            {'splitter': list(new_splitters), 'index': range(0, 3)},
            {'text': [None],},
            {'splitter': [None], 'index': range(0, 4),
               'start': range(0, min(block_len - 1, 7)), 'end': range(1, min(block_len, 8))},
        ],
        'param_gen': [ # optional
            #[(dest_block, dest_key), {}]
            [(1, 'text'), {'action': 'eq', 'params': {'src_block_idx': 0, 'src_key': 'splitter'} }],
            [(2, 'splitter'), {'action': 'eq', 'params': {'src_block_idx': 0, 'src_key': 'splitter'} }],
        ]
    }
