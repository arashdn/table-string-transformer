import time

from pattern.Generalizer.Generalizer import Generalizer
from Transformation.Pattern import *
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock


# ARRAY_MINUS = False

SIMILARITY_THRESHOLD = 0.5


def generate_similar_patterns(all_patterns, rows, generalize, verbose=True):
    patterns = list(all_patterns)
    if verbose:
        print(f"Applying {len(patterns)} patterns...")
    pat_inp = [set() for i in range(len(patterns))]
    pat_sim = [set() for i in range(len(patterns))]
    pat_sim_blocks = [set() for i in range(len(patterns))]

    inputs = []
    for inp, outs in rows.items():
        for out in outs:
            inputs.append((inp, out))
    inp_pat = [set() for i in range(len(inputs))]


    pattern_counts = [len(patterns) * len(inputs), 0, 0]  # total patterns, successful, hit cache

    generalization_candidates = set()
    cnt, all_cnt = 0, len(inputs)
    for inp_idx, inp in enumerate(inputs):
        if verbose and cnt % 10 == 0:
            print(f"Applying on input {cnt} / {all_cnt}")
        cnt += 1

        wrongs = set()  # MUST be mutable

        for pat_idx, pattern in enumerate(patterns):

            if generalize:
                o = pattern.apply_get_single_diff_block(inp[0], inp[1])
                if o[0] == inp[1]:
                    pat_inp[pat_idx].add(inp_idx)
                    inp_pat[inp_idx].add(pat_idx)
                elif is_similar_output(o):
                    generalization_candidates.add(pat_idx)
                    pat_sim[pat_idx].add(inp_idx)
                    pat_sim_blocks[pat_idx].add(o[1]['diff_block'])
            else:
                if pattern.fast_apply(inp[0], inp[1], wrongs, pattern_counts) == inp[1]:
                    pat_inp[pat_idx].add(inp_idx)
                    inp_pat[inp_idx].add(pat_idx)
                    pattern_counts[1] += 1

    if verbose:
        print(f"Applying Done! Patterns to be generalized: {len(generalization_candidates)} / {len(pat_sim)}")
        # for pat_idx in generalization_candidates:
        #     print(f"{patterns[pat_idx]}\n----------------")

    gen_start_time = time.time()
    effective_gens = 0
    if generalize:
        generalizer = Generalizer(inputs, patterns, pat_sim, pat_sim_blocks, pat_inp, verbose)
        # candidates = sorted(list(generalization_candidates))  # remove sort
        candidates = list(generalization_candidates)

        cnt = 0
        for cand in candidates:
            if verbose:
                cnt += 1
                print(f"Generalizing pat {cnt}/{len(candidates)}")
            generalizer.generalize(cand)

        assert len(generalizer.new_patterns) == len(generalizer.new_pattern_inp)
        for pattern, coverage in zip(generalizer.new_patterns, generalizer.new_pattern_inp):
            patterns.append(pattern)
            pat_inp.append(coverage)
            pat_idx = len(patterns) - 1
            assert patterns[pat_idx] == pattern
            for cov in coverage:
                inp_pat[cov].add(pat_idx)

        assert len(patterns) == len(pat_inp)
        effective_gens = len(generalizer.new_patterns)
        if verbose:
            print(f"{effective_gens} effective generalized patterns")

    gen_end_time = time.time()
    gen_time = gen_end_time - gen_start_time

    return inputs, pat_inp, inp_pat, patterns, gen_time, effective_gens, pattern_counts


def is_similar_output(o):
    return o[1] is not None and (
            get_dist(o[1]['diff_text'], o[1]['diff_out']) > 1 - SIMILARITY_THRESHOLD
            or o[1]['diff_text'] == '')


def get_dist(trans, out):
    from difflib import SequenceMatcher
    match = SequenceMatcher(None, trans, out).find_longest_match(0, len(trans), 0, len(out))
    # string1[match.a: match.a + match.size]
    return match.size/max(len(out), len(trans))


def get_covering_set(inputs, pat_inp, patterns, verbose=True):
    if verbose:
        print("Getting a covering set...")
    elements = set(range(len(inputs)))
    covered = set()
    cover = []

    # z = [(y,x) for y,x in sorted(zip(patterns,pat_inp))]
    # pat_inp = [x[1] for x in z]
    # patterns = [x[0] for x in z]

    # Greedily add the subsets with the most uncovered points
    while covered != elements:
        max_len = -1
        max_idx = None
        for idx, s in enumerate(pat_inp):
            if len(s - covered) > max_len:
                max_len = len(s - covered)
                max_idx = idx
        # subset = max(pat_inp, key=lambda s: len(s - covered))

        subset = pat_inp[max_idx]
        cover.append(max_idx)
        covered |= subset

    if verbose:
        print("Done! Just replacing single coverage patterns with literals...")

    final_res = [(
                    idx,
                    len(pat_inp[idx]),
                    merge_literals(patterns[idx]),
                    [inputs[i] for i in pat_inp[idx]]
                  ) for idx in cover]


    for idx, res in enumerate(final_res):
        if res[1] == 1:
            coverage = list(pat_inp[res[0]])
            text = inputs[coverage[0]][1]
            assert len(coverage) == 1
            # old_pat = final_res[idx][2]
            new_pat = Pattern([LiteralPatternBlock(text)])
            final_res[idx] = (None, 1, new_pat, res[3], res[2])
            # print(f"{old_pat} --> {new_pat}")

    return final_res


def merge_literals(pt):
    blks = []
    txt = ""
    for bl in pt.blocks:
        if type(bl) == LiteralPatternBlock:
            txt += bl.text
        else:
            if txt != "":
                blks.append(LiteralPatternBlock(txt))
                txt = ""
            blks.append(bl)
    if txt != "":
        blks.append(LiteralPatternBlock(txt))

    return Pattern(blks)
