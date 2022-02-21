from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock


def add_one_char_split(parent_idx, block_idx, inputs, patterns, pat_sim, pat_inp):
    # print("add_one_char_split")
    block = patterns[parent_idx].blocks[block_idx]
    # print(block)

    assert type(block) == TokenPatternBlock
    new_splitters = set()

    # We are looking for characters that have already happened after current separator.
    # Thus, we split the input and choose first character.

    candidates = set(pat_inp[parent_idx]) # we need to cover already covered inputs, so, we only look in already matched blocks
    # candidates.update(pat_sim[parent_idx])

    for cad in candidates:
        inp = inputs[cad][0]
        tmp = inp.split(block.splitter)
        for t in tmp:
            if len(t) > 0:
                new_splitters.add(block.splitter + t[0])

    return {
        'generalization_type': 'add_one_char_split',
        'blocks': [TokenPatternBlock(None, None), ],
        'params_space': [{'splitter': list(new_splitters), 'index': range(0, 4)}]
    }
