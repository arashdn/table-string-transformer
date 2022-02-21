import copy
import re
from difflib import SequenceMatcher

from data_processor import Matcher
from Transformation.Pattern import BasicPattern, BasicPatternBlock


def get_placeholders(rows, max_tokens, token_splitters=None, verbose=True, options={}, stats={}):

    # In addition to breaking all blocks at the same time, each block will be broken separately too
    break_each_block_separate = False
    if 'break_each_block_separate' in options:
        break_each_block_separate = options['break_each_block_separate']

    add_literals = True
    if 'add_literals' in options:
        add_literals = options['add_literals']

    only_first_match = False
    if 'only_first_match' in options:
        only_first_match = options['only_first_match']

    max_blocks = 10000
    if 'max_blocks' in options:
        max_blocks = options['max_blocks']

    all_patterns = set()
    sum_init_pat = 0
    sum_spt_pat = 0
    cnt = 0

    sum_removed_init_pat = 0
    sum_removed_splitted_pat = 0

    # i = 1
    for inp, outs in rows.items():
        # print(f"{i}/{len(rows.items())} inputs done on placeholder found...")
        # print(f"{inp} -> {outs}")
        # i+=1
        for out in outs:
            # if verbose:
            #     print(f"{inp} -> {out}")

            stats_pat = {'cnt_removed': 0}
            stats_spt = {'cnt_removed': 0}

            res = extract_placeholder_patterns(inp, out, max_tokens, max_blocks, add_literals, only_first_match, stats_pat)
            sum_init_pat += len(res)
            sum_removed_init_pat += stats_pat['cnt_removed']

            split_res = set()
            if token_splitters is not None:
                split_res = split_placeholders(res, token_splitters, inp, max_tokens, max_blocks, break_each_block_separate, stats_spt)

            sum_spt_pat += len(split_res)
            sum_removed_splitted_pat += stats_spt['cnt_removed']

            cnt += 1
            all_patterns.update(res)
            all_patterns.update(split_res)

            # print(len(res))

            # ext_pattern = list(res)
            # ext_pattern.sort(reverse=True)
            # print("*****************************")
            # for p in ext_pattern:
            #     print(p)
            # print("*****************************")
            # ext_pattern = list(split_res)
            # ext_pattern.sort(reverse=True)
            # # print("*****************************")
            # for p in ext_pattern:
            #     print(p)
            # print("*****************************")
        # break
    if verbose:
        print(f"{sum_init_pat} init patterns + {sum_spt_pat} splitted patterns = {sum_init_pat+sum_spt_pat}, {len(all_patterns)} unique, extracted from {cnt} input rows")


    stats['cnt_all_generated_placeholder_comb'] = sum_init_pat+sum_spt_pat+sum_removed_init_pat + sum_removed_splitted_pat
    stats['cnt_all_remaining_placeholder_comb'] = sum_init_pat + sum_spt_pat
    stats['cnt_all_removed_placeholder_comb'] = sum_removed_init_pat + sum_removed_splitted_pat
    stats['cnt_init_remaining_placeholder_comb'] = sum_init_pat
    stats['cnt_splitted_remaining_placeholder_comb'] = sum_spt_pat
    stats['cnt_removed_init_placeholder_comb'] = sum_removed_init_pat
    stats['cnt_removed_splitted_placeholder_comb'] = sum_removed_splitted_pat

    return all_patterns



def split_placeholders(res, token_splitters, inp, max_tokens, max_blocks, break_each_block_separate, stats={}):
    split_res = set()
    cnt_removed = 0

    for parent in res:
        pat = copy.deepcopy(parent)
        added_blocks = 0
        for i, blk in enumerate(parent.blocks):
            if blk.type == BasicPatternBlock.TYPE_TOKEN:
                for sp in token_splitters:
                    lst = blk.text.split(sp)
                    if len(lst) > 1:
                        new_blks = []
                        st = blk.start
                        for s in lst:
                            new_st = st + len(s) + len(sp)
                            if len(s) > 0:
                                begin_sep = None if st == 0 else inp[st - 1]
                                end_sep = None if st + len(s) == len(inp) else inp[st + len(s)]
                                new_blks.append(BasicPatternBlock(s, BasicPatternBlock.TYPE_TOKEN,
                                                                  st, st + len(s), begin_sep, end_sep))
                            new_blks.append(BasicPatternBlock(inp[st + len(s):new_st], BasicPatternBlock.TYPE_STR))
                            st = new_st
                        new_blks = new_blks[:-1]
                        pat.replace(i + added_blocks, new_blks)
                        added_blocks += len(new_blks) - 1
                        if break_each_block_separate:
                            pat2 = copy.deepcopy(parent)
                            pat2.replace(i, new_blks)
                            split_res.add(pat2)

        if pat.num_tokens <= max_tokens and len(pat) <= max_blocks:
            split_res.add(pat)
        else:
            cnt_removed += 1

    stats['cnt_removed'] = cnt_removed
    return split_res


def extract_placeholder_patterns(inp, out, max_tokens, max_blocks, add_literals=True, only_first_match=False, stats={}):
    final_pattern_pile = set()

    # Add the basic literal
    final_pattern_pile.add(BasicPattern(inp, out, [BasicPatternBlock(out, BasicPatternBlock.TYPE_STR)]))


    pts = extract_non_overlap_placeholder(inp, out, max_tokens, max_blocks, add_literals, only_first_match)

    cnt_removed = 0

    for p in pts:
        pat = BasicPattern(inp, out, p)
        # pat = BasicPattern.merge_literals(pat) # Already done in extract_non_overlap_placeholder

        if pat.num_tokens <= max_tokens and len(pat) <= max_blocks:
            final_pattern_pile.add(pat)
        else:
            cnt_removed += 1

    stats['cnt_removed'] = cnt_removed
    return final_pattern_pile


def extract_non_overlap_placeholder(inp, out, max_tokens, max_blocks, add_literals=True, only_first_match=False):
    match = SequenceMatcher(None, inp, out).find_longest_match(0, len(inp), 0, len(out))
    if match is None or match.size == 0:
        if len(out) > 0:
            return [
                [BasicPatternBlock(out, BasicPatternBlock.TYPE_STR)]
            ]
        else:
            return [
                []
            ]
    else:
        txt = inp[match.a:match.a + match.size]

        if only_first_match:
            matches = [match.a]
        else:
            matches = [m.start() for m in re.finditer('(?='+re.escape(txt)+')', inp)]


        pats = []
        for m in matches:
            begin_sep = None if m == 0 else inp[m - 1]
            end_sep = None if m + match.size == len(inp) else inp[m + match.size]

            pats.append(BasicPatternBlock(txt, BasicPatternBlock.TYPE_TOKEN, m, m + match.size, begin_sep, end_sep))


        if add_literals:
            pats.append(BasicPatternBlock(txt, BasicPatternBlock.TYPE_STR))

        lefts = [ [] ] if match.b == 0 else extract_non_overlap_placeholder(inp, out[0:match.b], max_tokens, max_blocks, add_literals, only_first_match)
        rights = [ [] ] if match.b + match.size == len(out) else extract_non_overlap_placeholder(inp, out[match.b + match.size:len(out)], max_tokens, max_blocks, add_literals, only_first_match)

        res = []

        for left in lefts:
            for right in rights:
                for pat in pats:
                    t = left + [pat] + right
                    # res.append(t)
                    tt = BasicPattern.blocks_merge_literals(t)
                    if len(tt) <= max_blocks:
                        res.append(tt)
                    # res.append(BasicPattern.blocks_merge_literals(t))


        return res

''''@TODO: Has bug if we have two placeholders with same size: e.g.
'arash dargahi nobari': ['arash d. nobari']:
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: nobari-(13-20)-(i:None)], }
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: nobar-(13-19)-(i:i)], [tok:i-(12-13)-(h: )], }
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: nobar-(13-19)-(i:i)], [tok:i-(19-20)-(r:None)], }
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: -(5-6)-(h:d)], [tok:nobari-(14-20)-( :None)], }
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: -(13-14)-(i:n)], [tok:nobari-(14-20)-( :None)], }
{[tok:arash d-(0-7)-(None:a)], [str:. ], [tok:nobari-(14-20)-( :None)], }
{[tok:arash d-(0-7)-(None:a)], [str:.], [tok: nobar-(13-19)-(i:i)], [str:i], }
{[tok:arash d-(0-7)-(None:a)], [str:. nobari], }
{[str:arash d.], [tok: nobari-(13-20)-(i:None)], }
{[str:arash d. nobari], }
'''
'''
def extract_placeholder_patterns_old(inp, out, max_tokens):
    pat = BasicPattern(inp, out, [BasicPatternBlock(out, BasicPatternBlock.TYPE_STR)])
    pattern_pile = [pat]

    final_pattern_pile = set()
    final_pattern_pile.add(pat)

    max_gram = min(len(inp), len(out))
    for n in range(max_gram, 0, -1):
        for main_idx, parent in enumerate(pattern_pile):
            if parent is None:
                continue
            if sum(1 for b in parent.blocks if b.type == BasicPatternBlock.TYPE_TOKEN) >= max_tokens:
                continue
            inp = parent.inp
            for i, blk in enumerate(parent.blocks):
                if blk.type == BasicPatternBlock.TYPE_STR:
                    text = blk.text
                    gggg = Matcher.get_qgrams(n, text)
                    # sort and making list are neither a part of algorithm nor required,
                    # but it is essential to have same result on all runs
                    grams = list(gggg)
                    grams.sort()
                    for gram in grams:
                        esc_gram = re.escape(gram)
                        occ = [(m.start(), m.end()) for m in re.finditer(esc_gram, text)]
                        assert len(occ) > 0
                        inp_occ = [(m.start(), m.end()) for m in re.finditer(esc_gram, inp)]
                        if len(inp_occ) > 0:
                            for ioc in inp_occ:
                                pattern = copy.deepcopy(parent)
                                new = []
                                if occ[0][0] > 0:
                                    new.append(BasicPatternBlock(text[0:occ[0][0]], BasicPatternBlock.TYPE_STR))

                                for idx, oc in enumerate(occ):

                                    begin_sep = None if ioc[0] == 0 else inp[ioc[0] - 1]
                                    end_sep = None if ioc[1] == len(inp) else inp[ioc[1]]
                                    new.append(BasicPatternBlock(
                                        gram, BasicPatternBlock.TYPE_TOKEN, ioc[0], ioc[1], begin_sep, end_sep
                                    ))
                                    if idx + 1 != len(occ):
                                        if oc[1] != occ[idx + 1][0]:  # there is text between them
                                            new.append(BasicPatternBlock(text[oc[1]:occ[idx + 1][0]],
                                                                         BasicPatternBlock.TYPE_STR))

                                if occ[-1][1] != len(text):
                                    new.append(BasicPatternBlock(text[occ[-1][1]:], BasicPatternBlock.TYPE_STR))

                                pattern.replace(i, new)
                                if pattern not in final_pattern_pile:
                                    if sum(1 for b in pattern.blocks if b.type == BasicPatternBlock.TYPE_TOKEN) <= max_tokens:
                                        pattern_pile.append(pattern)
                                        final_pattern_pile.add(pattern)
                                        pattern_pile[main_idx] = None

    return final_pattern_pile

'''
