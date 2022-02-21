import copy
import itertools
import multiprocessing
import random
import time

from difflib import SequenceMatcher
from multiprocessing import Queue

from autojoin.OperatorResult import OperatorResult
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock
from Transformation.Blocks.SplitSplitSubstrPatternBlock import SplitSplitSubstrPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Pattern import Pattern

GAIN_TRESHOLD = 0.2
LEVEL_TRESHOLD = 3

MULTI_CORE = False
NUM_PROCESSORS = multiprocessing.cpu_count()//2

BLOCKS = [PositionPatternBlock, TokenPatternBlock, LiteralPatternBlock, SplitSubstrPatternBlock]
# BLOCKS = [PositionPatternBlock, SplitSubstrPatternBlock, LiteralPatternBlock, SplitSplitSubstrPatternBlock, ]


def auto_join(rows, subset_size, num_subsets, verbose=False, print_name='NO_NAME'):

    verbose_lev = 0
    if verbose == 'limited' or verbose == True:
        verbose_lev = 2
    elif verbose == 'yes':
        verbose_lev = 7
    elif verbose == 'full':
        verbose_lev = 10

    start_time = time.time()

    subsets = get_subsets(rows, subset_size, num_subsets)

    cnt = 0
    rets = []

    # if MULTI_CORE:
    #     q = Queue()
    #     processes = []
    #     for subset in subsets:
    #         if verbose_lev > 0:
    #             cnt += 1
    #             print(f"subset {cnt}/{num_subsets}")
    #
    #         p = multiprocessing.Process(target=try_learn_transform,
    #                                     args=(subset, 0, verbose_lev, q, f"{cnt}/{num_subsets}"))
    #         processes.append(p)
    #         p.start()
    #
    #         # trans = try_learn_transform(subset, 0, verbose_lev)
    #
    #         # if verbose_lev > 0:
    #         #     print(trans)
    #
    #     for p in processes:
    #         ret = q.get()  # will block
    #         rets.append(ret)
    #
    #     for process in processes:
    #         process.join()
    #
    if MULTI_CORE:
        data = []
        for subset in subsets:
            cnt += 1
            data.append((subset, 0, verbose_lev, f"{cnt}/{num_subsets}"))

        if verbose_lev > 0:
            print(f"Using {NUM_PROCESSORS} processes")
        pool = multiprocessing.Pool(processes=NUM_PROCESSORS)
        rets = pool.starmap(try_learn_transform, data)
        pool.close()
        pool.join()

        # processes = []
        # for subset in subsets:
        #     if verbose_lev > 0:
        #         cnt += 1
        #         print(f"subset {cnt}/{num_subsets}")
        #
        #     p = multiprocessing.Process(target=try_learn_transform,
        #                                 args=(subset, 0, verbose_lev, q, f"{cnt}/{num_subsets}"))
        #     processes.append(p)
        #     p.start()
        #
        #     # trans = try_learn_transform(subset, 0, verbose_lev)
        #
        #     # if verbose_lev > 0:
        #     #     print(trans)
        #
        # for p in processes:
        #     ret = q.get()  # will block
        #     rets.append(ret)
        #
        # for process in processes:
        #     process.join()
    else:
        for subset in subsets:
            if verbose_lev > 0:
                cnt += 1
                print(f"subset {cnt}/{num_subsets}")

            trans = try_learn_transform(subset, 0, verbose_lev)
            rets.append(trans)

            if verbose_lev > 2:
                print(trans)

    # print(rets)
    time_rank_start = time.time()

    trans_set = set()
    for ret in rets:
        if ret['transformation'] is not None:
            trans_set.add(Pattern(ret['transformation']))

    # print(trans_set)

    patterns = list(trans_set)
    pat_inp = [set() for i in range(len(patterns))]

    inputs = []
    for inp, outs in rows.items():
        for out in outs:
            inputs.append((inp, out))
    inp_pat = [set() for i in range(len(inputs))]

    for inp_idx, inp in enumerate(inputs):
        for pat_idx, pattern in enumerate(patterns):
            try:
                o = pattern.apply(inp[0])
            except Exception as e:
                o = None
            if o == inp[1]:
                pat_inp[pat_idx].add(inp_idx)
                inp_pat[inp_idx].add(pat_idx)

    full_pat_inp = []
    for idx, cover in enumerate(pat_inp):
        full_pat_inp.append([idx, cover])
    lst = sorted(full_pat_inp, reverse=True, key=lambda item: len(item[1]))
    ranked = [(patterns[cover[0]], len(cover[1])) for pat, cover in enumerate(lst)]

    runtime = time.time() - start_time
    rank_time = time.time() - time_rank_start

    return {
        'print_name': print_name,
        'subset_res': rets,
        'ranked': ranked,
        'full_time': sum(t['time'] for t in rets) + rank_time,
        'runtime': runtime,
        'rank_time': rank_time,
        'input_len': len(inputs),
        'covered': sum(1 for p in inp_pat if len(p) > 0),
        'subset_size': subset_size,
        'num_subsets': num_subsets,
        'gain_threshold': GAIN_TRESHOLD,
        'level_threshold': LEVEL_TRESHOLD,
        'blocks': [b.__name__ for b in BLOCKS],
    }



def get_subsets(rows, subset_size, num_subsets):
    all_rows = []

    for inp, outs in rows.items():
        for out in outs:
            all_rows.append((inp, out))

    res = []
    while len(res) < num_subsets:
        t = set(pair for pair in random.sample(all_rows, k=subset_size))

        assert len(t) == subset_size

        if t not in res:
            res.append(t)

    return res


def try_learn_transform(subset, level, verbose_lev=0, subset_id=""):
    if level == LEVEL_TRESHOLD:
        return None

    if level == 0:
        start_time = time.time()

        if verbose_lev > 0:
            print(f"Subset {subset_id} Started.")

    ops = find_best_logical_ops(subset)
    if verbose_lev > 6:
        cnt = 0
        all_cnt = sum([1 if op.gain > GAIN_TRESHOLD else 0 for op in ops])
    for op in ops:
        if verbose_lev > 6:
            cnt += 1
            print(f"  **S{subset_id}**{cnt}/{all_cnt}({len(ops)})")
        if op.gain == 1:
            if level == 0:
                if verbose_lev > 0:
                    print(f"Subset {subset_id} Done! ({[op.block]})")
                return {
                        'transformation': [op.block],
                        'subset': subset,
                        'time': time.time() - start_time
                    }
            return [op.block]
        if op.gain < GAIN_TRESHOLD:
            continue
        left_done = True
        right_done = True
        left_subset = set()
        right_subset = set()
        for pair in subset:
            gen = op.block.apply(pair[0])
            out = pair[1]
            assert len(gen) <= len(out)

            st_index = out.find(gen)
            if st_index == -1:
                break
            end_index = st_index + len(gen)

            left = out[0:st_index]
            if left != "":
                left_done = False
            right = out[end_index:]
            if right != "":
                right_done = False

            left_subset.add((pair[0], left))
            right_subset.add((pair[0], right))
        else:
            l_res = []
            r_res = []
            if not left_done:
                if verbose_lev > 8:
                    print("    Left")
                l_res = try_learn_transform(left_subset, level+1)
                if l_res is None:
                    continue
            if not right_done:
                if verbose_lev > 8:
                    print("    Right")
                r_res = try_learn_transform(right_subset, level+1)
                if r_res is None:
                    continue

            if level == 0:
                if verbose_lev > 0:
                    print(f"Subset {subset_id} Done! ({l_res + [op.block] + r_res})")

                return {
                        'transformation': l_res + [op.block] + r_res,
                        'subset': subset,
                        'time': time.time() - start_time
                    }
            return l_res + [op.block] + r_res

    if level == 0:
        if verbose_lev > 0:
            print(f"Subset {subset_id} Done!")

        return {
            'transformation': None,
            'subset': subset,
            'time': time.time() - start_time
        }
    return None


def find_best_logical_ops(subset):
    inps = []
    for pair in subset:
        inps.append(pair[0])
    outs = []
    for pair in subset:
        outs.append(pair[1])

    res = []

    for tr_block in BLOCKS:
        parameters = tr_block.get_param_space(outs) if tr_block == LiteralPatternBlock else tr_block.get_param_space(inps)

        p_list = []
        p_keys = []
        for key, value in parameters.items():
            p_keys.append(key)
            p_list.append(value)
        params = list(itertools.product(*p_list))

        for param in params:
            tr = tr_block()
            assert len(param) == len(p_keys)
            for key, value in zip(p_keys, param):
                setattr(tr, key, value)

            gain_sum = 0.0
            for pair in subset:
                try:
                    out = tr.apply(pair[0])
                    if len(out) > len(pair[1]):
                        break
                    gain_sum += get_gain(out, pair[1])
                except (IndexError, ValueError) as e:
                    break
            else:
                res.append(OperatorResult(gain_sum/len(subset), tr))

    res.sort(reverse=True)
    return res


def get_gain(trans, out):
    if len(trans) == 0 or len(out) < len(trans):
        return 0
    match = SequenceMatcher(None, trans, out).find_longest_match(0, len(trans), 0, len(out))
    # string1[match.a: match.a + match.size]
    return match.size/len(out)
