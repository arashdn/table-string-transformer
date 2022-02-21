import copy
import importlib
import itertools


def get_block_generalizations(blk_type):

    from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
    from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
    if blk_type == PositionPatternBlock:
        return [
            ('pattern.Generalizer.PositionGeneralizations', 'change_pos_split_splitsubstr')  # module, func
        ]
    elif blk_type == TokenPatternBlock:
        return [
            ('pattern.Generalizer.TokenGeneralizations', 'add_one_char_split')  # module, func
        ]
    else:
        return []


class Generalizer:

    def __init__(self, inputs, patterns, pat_sim, pat_sim_blocks, pat_inp, verbose=True):
        self.pat_sim_blocks = pat_sim_blocks
        self.pat_inp = pat_inp
        self.pat_sim = pat_sim
        self.patterns = patterns
        self.inputs = inputs
        self._new_patterns = []
        self._new_pattern_inp = []
        self.verbose = verbose

    @property
    def new_patterns(self):
        return self._new_patterns

    @property
    def new_pattern_inp(self):
        return self._new_pattern_inp

    def generalize(self, pat_idx):
        pattern = self.patterns[pat_idx]
        # print(pattern)
        blocks_to_extend = self.pat_sim_blocks[pat_idx]

        for block_idx in blocks_to_extend:
            block = pattern.blocks[block_idx]
            extensions = get_block_generalizations(type(block))
            for ext in extensions:
                module = importlib.import_module(ext[0])
                func = getattr(module, ext[1])
                params = func(pat_idx, block_idx, self.inputs, self.patterns, self.pat_sim, self.pat_inp)
                if params is not None:  # block is generalizable
                    self.param_searcher(pat_idx, block_idx, params)

    def param_searcher(self, parent_idx, block_idx, params):
        n = len(params['blocks'])
        assert len(params['params_space']) == n

        # param_list = [[] for i in range(n)]
        param_keys = [[] for i in range(n)]
        all_params = [[] for i in range(n)]

        for idx, (block, parameters) in enumerate(zip(params['blocks'], params['params_space'])):
            # print(f"{idx}: {block} -> {parameters}")
            p_list = []
            p_keys = []
            for key, value in parameters.items():
                p_keys.append(key)
                p_list.append(value)
            param_keys[idx] = p_keys
            # param_list[idx] = p_list
            all_params[idx] = list(itertools.product(*p_list))
            # all_params[idx] = list(map(list, itertools.product(*p_list)))

        # final_params = list(itertools.product(*all_params))
        tmp = list(itertools.product(*all_params))
        # tmp = list(map(list, itertools.product(*all_params)))

        # copy to list
        final_params = []
        for pp in tmp:
            m = []
            for p in pp:
                m.append(list(p))
            final_params.append(m)


        if 'param_gen' in params and len(params['param_gen']) > 0:
            for idx, paramset in enumerate(final_params):
                for gen in params['param_gen']:

                    blk_idx = gen[0][0]
                    key_idx = param_keys[blk_idx].index(gen[0][1])
                    action = gen[1]['action']
                    prs = gen[1]['params']

                    if action == 'eq':
                        src_blk_idx = prs['src_block_idx']
                        src_key_idx = param_keys[src_blk_idx].index(prs['src_key'])
                        val = paramset[src_blk_idx][src_key_idx]
                        old = final_params[idx][blk_idx][key_idx]
                        final_params[idx][blk_idx][key_idx] = val
                    else:
                        raise ValueError(f"action {action} not recognized")


        # print(all_params)
        # print(param_keys)
        # print(final_params)
        # exit()
        if self.verbose:
            print(f"   {params['generalization_type']} ({len(final_params)})")

        for paramset in final_params:
            pattern = copy.deepcopy(self.patterns[parent_idx])
            blocks = copy.deepcopy(params['blocks'])

            # print(paramset)
            assert len(paramset) == n


            for idx, param in enumerate(paramset):
                assert len(param) == len(param_keys[idx])
                for key, value in zip(param_keys[idx], param):
                    setattr(blocks[idx], key, value)
            # print(blocks)
            pattern.blocks[block_idx:block_idx+1] = blocks


            if pattern in self.new_patterns:
                continue

            # if paramset[0][0] == ' ':
            #     print(pattern)

            # First we apply the pattern to original inputs:
            for inp_idx in self.pat_inp[parent_idx]:
                inp = self.inputs[inp_idx]
                try:
                    ot = pattern.apply(inp[0])
                    if ot != inp[1]:
                        break
                except Exception:
                    break
            else:  # Pattern can cover all of its parent coverage and should be considered
                first_coverage = set(self.pat_inp[parent_idx])
                coverage = set(self.pat_inp[parent_idx])
                for inp_idx, inp in enumerate(self.inputs):
                    if inp_idx not in first_coverage:
                        try:
                            if pattern.apply(inp[0]) == inp[1]:
                                coverage.add(inp_idx)
                        except Exception:
                            pass

                if len(coverage) > len(first_coverage):
                    self.new_patterns.append(pattern)
                    self._new_pattern_inp.append(coverage)
