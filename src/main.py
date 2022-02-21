import argparse
import json
import multiprocessing
import os
import random
import shutil
import sys

from data_processor import DataLoader as dl
from data_processor import Matcher as matcher
from evaluator.ColumnMatcherEval import ColumnMatcherEval
from evaluator.RowMatcher import RowMatcherEval, RowMatcherUnit
from autojoin import autojoin as aj
from evaluator.TransformationSetEval import TransformationSetEval
from pattern import Finder

from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock
from Transformation.Blocks.TwoCharSplitSubstrPatternBlock import TwoCharSplitSubstrPatternBlock

import pathlib

BASE_PATH = str(pathlib.Path(__file__).absolute().parent.parent.absolute())

METHOD = "PT"

# MUST Be Kept updated in order for config file to work. It also should be updated in the Finder.py
PT_PARAMS = {
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
    # 'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock, TwoCharSplitSubstrPatternBlock],  # not including literal

}

GOLDEN_ROWS = True
SWAP_SRC_TARGET = True

DATASET = "BM"
DS_PATH = BASE_PATH + '/data/autojoin-Benchmark/'
# DS_PATH = BASE_PATH + '/data/autojoin-no-gt/' # must set GOLDEN_ROW to False
# DS_PATH = BASE_PATH + '/data/synthesis/'
# DATASET = "FF"
# DS_PATH = BASE_PATH + '/data/FlashFill/'


MULTI_CORE = True
NUM_PROCESSORS = 0  # 0: multiprocessing.cpu_count()//2

DO_REMAINING = True
OVERRIDE = True
OUTPUT_PATH = BASE_PATH + "/output/"

AJ_SUBSET_SIZE = 2
AJ_NUM_SUBSET = 8  # or '5%'

ROW_MATCHING_N_START = 4
ROW_MATCHING_N_END = 20

CNT_CUR = multiprocessing.Value('i', 0)
CNT_ALL = 0


def get_pattern(func, params, verbose=False):
    lst = []
    '''
    You can insert list of some tables in lst to limit loading to those tables, if lst=[], all tables are loaded
    e.g.:
    # lst = [
    #     'synthesis-50rows',
    #     'synthesis-500rows',
    #     'synthesis-5000rows',
    # ]
    '''

    tables, all_tables = dl.get_tables_from_dir(DS_PATH, lst, make_lower=True, verbose=False)

    print("Reading Done!")

    res = matcher.get_matching_tables_golden(tables, bidi=False)

    ''' This Part is for experimental column matching, still not complete and should be commented:

    # match_start_time = time.time()
    # res = matcher.get_matching_tables(all_tables, tables, q=6, matching_ratio=0.7,
    #                                   src_keys_ratio=0.2, tgt_keys_ratio=0.2, verbose=True)
    # match_end_time = time.time()
    # print("Matcher RunTime: --- %f seconds ---" % (match_end_time - match_start_time))
    #
    # cme = ColumnMatcherEval(all_tables, tables, res)
    # print(cme)

    # item = res['items'][1]
    # rows, is_swapped = matcher.get_matching_rows_golden(tables, item, swap_src_target=SWAP_SRC_TARGET)
    # Finder.get_patterns(rows, max_tokens=3)
    '''

    if DO_REMAINING and os.path.exists(OUTPUT_FILE):
        skip = 0
        items_new = []
        done_tbl = []
        with open(OUTPUT_FILE, "r") as f:
            for line in f.readlines():
                tab = line.strip().split(',')[0]
                if tab != "file_name":
                    done_tbl.append(tab)

        for item in res['items']:
            tbl_name = item['src_table'][4:]
            if tbl_name in done_tbl:
                assert os.path.exists(OUTPUT_DIR + tbl_name + ".txt")
                skip += 1
            else:
                items_new.append(item)

        print(f"{skip}/{len(res['items'])} already done.")
        res['items'] = items_new

    all_rows = []
    data = []

    if not MULTI_CORE:
        print("Running on single core mode")

    global CNT_ALL
    CNT_ALL = len(res['items'])

    all_has_gt = True

    for item in res['items']:
        print(f"Matching rows for '{item['src_table']}' ...")
        has_GT = 'GT' in tables[item['src_table'][4:]]
        all_has_gt = all_has_gt and has_GT

        if GOLDEN_ROWS:
            if not has_GT:
                raise Exception(
                    'golden row matching cannot be used when no ground truth table exists for ' + item['src_table'][4:])

            rows, is_swapped = matcher.get_matching_rows_golden(tables, item, swap_src_target=SWAP_SRC_TARGET)
        else:
            rows, is_swapped = matcher.get_matching_rows_for_table(tables, item, ROW_MATCHING_N_START, ROW_MATCHING_N_END,
                                                         swap_src_target=SWAP_SRC_TARGET)
        if verbose:
            print(f"source and target columns are {'' if is_swapped else 'NOT '}swapped")
        new_rows = {}
        for src, target in rows.items():
            new_rows[src] = [[t, t] for t in target]

        rr = {
            'col_info': item,
            'is_swapped': is_swapped,
            'rows': new_rows
        }
        all_rows.append(rr)

        rmu = None
        if has_GT:
            rmu = RowMatcherUnit(tables, rr)
            print(rmu)

        if func == 'run_pattern' and params['sample_size'] is not None:
            subset_size = params['sample_size']
            if not isinstance(subset_size, (list, tuple, set,)):
                subset_size = [subset_size]

            CNT_ALL += len(subset_size) - 1

            for size in subset_size:
                size_txt = size
                if type(size) == str:
                    assert size[-1] == '%'
                    percent = int(size[:-1])
                    a = float(percent) / 100
                    size = int(a * len(rows)) + 1
                    if size > len(rows):
                        size = len(rows)
                else:
                    size = int(size)

                keys = random.sample(list(rows), size)
                sample_rows = {}
                for k in keys:
                    sample_rows[k] = rows[k]

                if MULTI_CORE:
                    data.append((item, sample_rows, params, tables, item['src_table'][4:] + "_s-" + str(size_txt), rmu,
                                 verbose))
                else:
                    globals()[func](item, sample_rows, params, tables, item['src_table'][4:] + "_s-" + str(size_txt),
                                    rmu, verbose=verbose)
        else:
            if MULTI_CORE:
                data.append((item, rows, params, tables, None, rmu, verbose))
            else:
                globals()[func](item, rows, params, tables, None, rmu, verbose=verbose)

    if all_has_gt and len(all_rows) > 0:
        rme = RowMatcherEval(tables, all_rows)
        print("Row matching performance:" + str(rme))

    if MULTI_CORE:
        print(f"Using {NUM_PROCESSORS} processes...")

        # @TODO: Support spawn
        if sys.platform in ('win32', 'msys', 'cygwin'):
            print("fork based multi core processing works only on *NIX type operating systems.")
            sys.exit(1)

        from multiprocessing import get_context
        pool = get_context('fork').Pool(processes=NUM_PROCESSORS)
        # pool = multiprocessing.Pool(processes=NUM_PROCESSORS)

        rets = pool.starmap(globals()[func], data)
        pool.close()

        pool.join()


def run_pattern(item, rows, params, tables, table_name=None, rmu=None, verbose=None):
    if table_name is None:
        table_name = item['src_table'][4:]
    res = Finder.get_patterns(rows, params=params, table_name=table_name, verbose=verbose)

    if rmu is not None:
        tr_eval = TransformationSetEval(tables, item, [r[2] for r in res['patterns']], SWAP_SRC_TARGET)
    else:
        if verbose:
            print("No golden set provided")
        tr_eval = None

    global CNT_CUR
    with CNT_CUR.get_lock():
        CNT_CUR.value += 1
    print(f"({CNT_CUR.value}/{CNT_ALL}) -> " + table_name)
    print("Total run time: %.2f s" % res['runtime'])
    print(f"{len(res['patterns'])} patterns / {res['input_len']} inputs \n-----------")
    if verbose:
        print(tr_eval)
    pt_print(res, table_name, rmu, tr_eval)


def run_aj(item, rows, params, tables, table_name=None, rmu=None, verbose=False):
    # Verbose = limited,yes,full

    if table_name is None:
        table_name = item['src_table'][4:]
    tbl = table_name

    num_subsets = params['num_subsets']
    subset_size = params['subset_size']

    '''
    if isinstance(subset_size, (list, tuple, set,)):
        if PARAM_MULTI_CORE:
            prm = []
            for size in subset_size:
                assert not isinstance(subset_size, (list, tuple, set,))
                prm.append((item, rows, {'subset_size': size, 'num_subsets': num_subsets}, rmu, verbose, tbl + f"_subsize_{size}"))
            # print(f"Using {PARAM_NUM_PROCESSORS} processes for params...")
            pool = multiprocessing.Pool(processes=PARAM_NUM_PROCESSORS)
            rets = pool.starmap(run_aj, prm)
            pool.close()
            pool.join()
            for res in rets:
                print(f": {res['print_name']} Done in %.2f s" % res['runtime'])
                aj_print(res, res['print_name'], rmu)
        else:
            i = 0
            for size in subset_size:
                i += 1
                assert not isinstance(size, (list, tuple, set,))
                res = aj.auto_join(rows, size, num_subsets, verbose=verbose, print_name=tbl + f"_subsize_{size}")
                print(f"Param {i}/{len(subset_size)}: {res['print_name']} Done in %.2f s" % res['runtime'])
                aj_print(res, res['print_name'], rmu) 
    else:
        res = aj.auto_join(rows, subset_size, num_subsets, verbose=verbose, print_name=tbl)
        print(f"{res['print_name']}")
        print("Total time: %.2f s" % res['full_time'] + ", Total spent time: %.2f s" % res['runtime'])
        print("---------")
        aj_print(res, res['print_name'], rmu)


'''

    if type(subset_size) == str:
        assert subset_size[-1] == '%'
        percent = int(subset_size[:-1])
        a = float(percent) / 100
        subset_size = int(a * len(rows)) + 1
        if subset_size > len(rows):
            subset_size = len(rows)
        print(f"{tbl} -> Subset size: {percent}% = {subset_size}")

    res = aj.auto_join(rows, subset_size, num_subsets, verbose=verbose, print_name=tbl)

    if rmu is not None:
        tr_eval = TransformationSetEval(tables, item, [r[0] for r in res['ranked']], SWAP_SRC_TARGET)
    else:
        if verbose:
            print("No golden set provided")
        tr_eval = None

    global CNT_CUR
    with CNT_CUR.get_lock():
        CNT_CUR.value += 1
    print(f"({CNT_CUR.value}/{CNT_ALL}) -> {res['print_name']}")
    print("Total time: %.2f s" % res['full_time'] + ", Total spent time: %.2f s" % res['runtime'])
    print("---------")
    if verbose:
        print(tr_eval)
    aj_print(res, res['print_name'], rmu, tr_eval)


def pt_print(res, filename, row_matcher, tr_eval):
    coverage = res['covered'] / res['input_len'] if res['input_len'] != 0 else "NA"
    best_coverage = res['ranked'][0][1] / res['input_len'] if len(res['ranked']) > 0 else 0
    total_patterns = len(res['ranked'])
    input_rows = len(tr_eval.inp_pat) if tr_eval is not None else "NA"

    if not os.path.exists(OUTPUT_FILE):
        pathlib.Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_FILE, "a+") as f:
            print(
                "file_name,Input Rows,avg_len_input,avg_len_output,Matched pairs,Total Patterns," +
                "Golden Coverage,Golden Best tran Coverage,Coverage,Best tran Coverage," +
                "Runtime,Effective Generalizations,Generalization Time,Row_Match_P,Row_Match_R,Row_Match_F1," +
                "all_generated_placeholder_comb,all_removed_placeholder_comb,all_remaining_placeholder_comb,init_remaining_placeholder_comb,splitted_remaining_placeholder_comb," +
                "placeholder_gen_time,extract_pat_time,duplicate_pat_remove_time,pat_applying_time,get_covering_set_time," +
                "Num placeholder comb,Num All Patterns,Num duplicate patterns removed,Num patterns to try," +
                "cnt_all_patterns_all_rows,cnt_patterns_successful,cnt_patterns_failed,cnt_patterns_hit_cache"
                , file=f)

    with open(OUTPUT_FILE, "a+") as f:
        rows = f"{row_matcher.precision},{row_matcher.recall},{row_matcher.f1}" if row_matcher is not None else "-1,-1,-1"
        tr_eval_txt = f"{tr_eval.coverage},{tr_eval.best_pattern_coverage}," if tr_eval is not None else "NA,NA,"
        print(
            f"{filename},{input_rows},{res['avg_len_input']},{res['avg_len_output']},{res['input_len']},{total_patterns}," +
            tr_eval_txt +
            f"{coverage},{best_coverage}," +
            f"{res['runtime']},{res['effective_gens']},{res['gen_time']}," +
            rows + "," +
            f"{res['cnt_all_generated_placeholder_comb']},{res['cnt_all_removed_placeholder_comb']},{res['cnt_all_remaining_placeholder_comb']},{res['cnt_init_remaining_placeholder_comb']},{res['cnt_splitted_remaining_placeholder_comb']}," +
            f"{res['placeholder_gen_time']},{res['extract_pat_time']},{res['duplicate_pat_remove_time']},{res['pat_applying_time']},{res['get_covering_set_time']}," +
            f"{res['cnt_placeholder_comb']},{res['cnt_all_patterns']},{res['cnt_dup_patterns_removed']},{res['cnt_pattern_remaining']}," +
            f"{res['cnt_all_patterns_all_rows']},{res['cnt_patterns_successful']},{res['cnt_patterns_failed']},{res['cnt_patterns_hit_cache']}"
            , file=f)

    with open(OUTPUT_DIR + filename + ".txt", "a+") as f:
        print("---------------", file=f)
        print(f"Results for {filename}", file=f)
        if tr_eval is not None:
            print(f"Coverage in golden matched rows: {tr_eval.covered}/{len(tr_eval.inp_pat)} = %.2f%%" % (
                        tr_eval.covered * 100 / len(tr_eval.inp_pat)), file=f)
            print(f"Best pattern Coverage  in golden matched rows: %.2f%%" % (tr_eval.best_pattern_coverage * 100),
                  file=f)

        if res['input_len'] != 0:
            print(f"Coverage: {res['covered']}/{res['input_len']} = %.2f%%" % (res['covered'] * 100 / res['input_len']),
                  file=f)
        else:
            print(f"Coverage: NA", file=f)

        print(f"Best pattern Coverage: %.2f%%" % (best_coverage * 100), file=f)
        print(f"Total Patterns: {total_patterns}, input rows: {res['input_len']}", file=f)
        print(
            f"Average Input length (Num chars): {res['avg_len_input']}, Average output length (Num chars): {res['avg_len_output']}",
            file=f)
        print(f"Max Tokens: {res['max_tokens']}", file=f)
        print(f"Total Run time: {res['runtime']}, Generalization time:{res['gen_time']}", file=f)
        print(f"Total number of placeholder combinations (After removing duplicates): {res['cnt_placeholder_comb']}",
              file=f)
        print(f"Total number of all generated patterns: {res['cnt_all_patterns']}", file=f)
        print(f"Number of duplicate patterns removed: {res['cnt_dup_patterns_removed']}", file=f)
        print(f"Total number of generated patterns to try: {res['cnt_pattern_remaining']}", file=f)
        print(f"Effective Generalizations: {res['effective_gens']}", file=f)

        print(f"\nNumber of all generated placeholder comb.: {res['cnt_all_generated_placeholder_comb']}", file=f)
        print(f"Number of all remaining placeholder comb.: {res['cnt_all_remaining_placeholder_comb']}", file=f)
        print(f"Number of all removed placeholder comb.: {res['cnt_all_removed_placeholder_comb']}", file=f)
        print(f"Number of init remaining placeholder comb.: {res['cnt_init_remaining_placeholder_comb']}", file=f)
        print(f"Number of splitted remaining placeholder comb.: {res['cnt_splitted_remaining_placeholder_comb']}",
              file=f)
        print(f"Number of removed init placeholder comb.: {res['cnt_removed_init_placeholder_comb']}", file=f)
        print(f"Number of removed splitted placeholder comb.: {res['cnt_removed_splitted_placeholder_comb']}", file=f)

        print(f"\nplaceholder generation time: {res['placeholder_gen_time']}", file=f)
        print(f"extract patterns time: {res['extract_pat_time']}", file=f)
        print(f"duplicate patterns remove time: {res['duplicate_pat_remove_time']}", file=f)
        print(f"patterns applying time: {res['pat_applying_time']}", file=f)
        print(f"get covering set time: {res['get_covering_set_time']}", file=f)

        print(
            f"\nNumber of all patterns that applied on all rows (#pattern * #rows) (complexity): {res['cnt_all_patterns_all_rows']}",
            file=f)
        print(f"Number of successful patterns: {res['cnt_patterns_successful']}", file=f)
        print(f"Number of failed patterns: {res['cnt_patterns_failed']}", file=f)
        print(f"Number of failed patterns filtered by cache (cache hit): {res['cnt_patterns_hit_cache']}", file=f)

        s = "\nGolden" if GOLDEN_ROWS else "N-gram"
        if row_matcher is not None:
            print(f"{s} Row Matching: P:{row_matcher.precision},R:{row_matcher.recall},F:{row_matcher.f1}", file=f)
        print(f"Params: {res['params']}", file=f)
        print("pattern list: {", file=f)
        s = ""
        for tr in res['ranked']:
            s += "                 "
            old_pat = ""
            if len(tr) == 5:
                old_pat = f"  -- Original Trans.: {tr[4]}"
            s += str(tr[2]) + f" -> {tr[1]}/{res['input_len']}{old_pat}\n"
            for inp in tr[3]:
                s += "                     |->" + str(inp) + "\n"

        print(s, file=f)

        if tr_eval is not None:
            print("****Golden rows apply patterns******", file=f)
            print("golden pattern list: {", file=f)
            s = ""
            for num, tr in enumerate(tr_eval.transformation_list):
                s += "                 "
                s += str(tr) + f" -> {len(tr_eval.pat_inp[num])}/{len(tr_eval.inp_pat)}\n"
                for inp in tr_eval.pat_inp[num]:
                    s += "                     |->" + str(tr_eval._inputs[inp]) + "\n"

            print(s, file=f)

            print("--- Not covered inputs:", file=f)
            for i, inp in enumerate(tr_eval.inp_pat):
                if len(inp) == 0:
                    print("   " + str(tr_eval._inputs[i]), file=f)

        print("****************", file=f)


def aj_print(res, filename, row_matcher, tr_eval):
    best_coverage = res['ranked'][0][1] / res['input_len'] if len(res['ranked']) > 0 else 0
    total_patterns = len(res['ranked'])
    input_rows = len(tr_eval.inp_pat) if tr_eval is not None else "NA"

    if not os.path.exists(OUTPUT_FILE):
        pathlib.Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_FILE, "a+") as f:
            print(
                "file_name,Input Rows,Matched pairs,Total Patterns," +
                "Golden Coverage,Golden Best tran Coverage,Coverage,Best tran Coverage," +
                "Required Runtime,Execution time,Ranking time," +
                "rows per subset,number of subsets,level threshold,gain_threshold," +
                "Row_Match_P,Row_Match_R,Row_Match_F1",
                file=f)

    with open(OUTPUT_FILE, "a+") as f:
        rows = f"{row_matcher.precision},{row_matcher.recall},{row_matcher.f1}" if row_matcher is not None else "-1,-1,-1"
        tr_eval_txt = f"{tr_eval.coverage},{tr_eval.best_pattern_coverage}," if tr_eval is not None else "NA,NA,"
        print(f"{filename},{input_rows},{res['input_len']},{total_patterns}," +
              tr_eval_txt +
              f"{res['covered'] / res['input_len']},{best_coverage}," +
              f"{res['full_time']},{res['runtime']},{res['rank_time']}," +
              f"{res['subset_size']},{res['num_subsets']},{res['level_threshold']},{res['gain_threshold']}," +
              rows
              , file=f)

    with open(OUTPUT_DIR + filename + ".txt", "a+") as f:
        print("---------------", file=f)
        print(f"Results for {filename}", file=f)
        if tr_eval is not None:
            print(f"Coverage in golden matched rows: {tr_eval.covered}/{len(tr_eval.inp_pat)} = %.2f%%" % (
                        tr_eval.covered * 100 / len(tr_eval.inp_pat)), file=f)
            print(f"Best pattern Coverage  in golden matched rows: %.2f%%" % (tr_eval.best_pattern_coverage * 100),
                  file=f)
        print(f"Coverage: {res['covered']}/{res['input_len']} = %.2f%%" % (res['covered'] * 100 / res['input_len']),
              file=f)
        print(f"Best pattern Coverage: %.2f%%" % (best_coverage * 100), file=f)
        print(f"Total Patterns: {total_patterns}, input rows: {res['input_len']}", file=f)
        print(f"Total required runtime: {res['full_time']}", file=f)
        print(f"Execution time: {res['runtime']}, Ranking time:{res['rank_time']}", file=f)
        print(f"rows per subset: {res['subset_size']}, number of subsets: {res['num_subsets']}", file=f)
        print(f"level threshold: {res['level_threshold']}, gain_threshold: {res['gain_threshold']}", file=f)
        s = "Golden" if GOLDEN_ROWS else "N-gram"
        if row_matcher is not None:
            print(f"{s} Row Matching: P:{row_matcher.precision},R:{row_matcher.recall},F:{row_matcher.f1}", file=f)
        print(f"blocks: {res['blocks']}", file=f)
        print("ranked list: {", file=f)
        s = ""
        for tr in res['ranked']:
            s += "                 "
            s += str(tr[0]) + f" -> {tr[1]}/{res['input_len']}\n"

        print(s, file=f)
        print("             }", file=f)

        print("subset list: {", file=f)
        s = ""
        for tr in res['subset_res']:
            s += "                 {\n"
            s += f"                     subset:{tr['subset']}\n"
            s += f"                     transformation:{tr['transformation']}\n"
            s += f"                     time:{tr['time']}\n"
            s += "                 }\n"

        print(s, file=f)
        print("             }", file=f)
        print("****************", file=f)


def row_matching_test(n_start_from=2, n_start_to=25, file_write=True):
    if file_write:
        if not os.path.exists(OUTPUT_FILE):
            pathlib.Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_FILE, "a+") as f:
                print("min_n,p,r,f1", file=f)

    lst = []
    tables, all_tables = dl.get_tables_from_dir(DS_PATH, lst, make_lower=True, verbose=False)
    print("Reading Done!")

    res = matcher.get_matching_tables_golden(tables, bidi=False)

    for n_start in range(n_start_from, n_start_to):
        file_path = OUTPUT_DIR + f"/n_start_{n_start:02d}.csv"
        all_rows = []

        for item in res['items']:
            print(f"Matching rows for '{item['src_table']}' ...")

            rows, is_swapped = matcher.get_matching_rows_for_table(tables, item, n_start, ROW_MATCHING_N_END,
                                                         swap_src_target=SWAP_SRC_TARGET)
            new_rows = {}
            for src, target in rows.items():
                new_rows[src] = [[t, t] for t in target]

            rr = {
                'col_info': item,
                'is_swapped': is_swapped,
                'rows': new_rows
            }
            all_rows.append(rr)

            rmu = RowMatcherUnit(tables, rr)
            print(rmu)
            if file_write:
                if not os.path.exists(file_path):
                    with open(file_path, "a+") as f1:
                        print("table,rows,tp,fp,fn,p,r,f", file=f1)
                with open(file_path, "a+") as f1:
                    print(
                        f"{item['src_table'][4:]},{rmu.tp + rmu.fn},{rmu.tp},{rmu.fp},{rmu.fn},{rmu.precision},{rmu.recall},{rmu.f1}"
                        , file=f1)

        rme = RowMatcherEval(tables, all_rows)
        print("Row matching performance:" + str(rme))
        if file_write:
            with open(OUTPUT_FILE, "a+") as f:
                print(f"{n_start},{rme.precision},{rme.recall},{rme.f1}", file=f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c', action='store', type=str, required=False,
                        default='', help='Path of config file')
    args = parser.parse_args()
    if args.config != '':
        cnf_path = str(pathlib.Path(args.config).absolute())
        print(f"Loading config file: {cnf_path}")
        with open(cnf_path, "r") as f:
            cnf = json.load(f)
        print(cnf)
        MULTI_CORE = cnf.get('multicore', MULTI_CORE)
        NUM_PROCESSORS = cnf.get('num_processors', NUM_PROCESSORS)
        GOLDEN_ROWS = cnf.get('golden_rows', GOLDEN_ROWS)
        DO_REMAINING = cnf.get('do_remaining', DO_REMAINING)
        OVERRIDE = cnf.get('override', OVERRIDE)
        DS_PATH = cnf.get('dataset_path', DS_PATH)
        OUTPUT_PATH = cnf.get('output_path', OUTPUT_PATH)
        if cnf.get('add_base_path', True):
            DS_PATH = BASE_PATH + DS_PATH
            OUTPUT_PATH = BASE_PATH + OUTPUT_PATH
        DATASET = cnf.get('dataset', DATASET)
        METHOD = cnf.get('method', METHOD)
        AJ_NUM_SUBSET = cnf.get('aj_num_subset', AJ_NUM_SUBSET)
        AJ_SUBSET_SIZE = cnf.get('aj_subset_size', AJ_SUBSET_SIZE)

        params_diff_list = ['units_to_extract']

        pt_params_cnf = cnf.get('pt_params', {})
        for key in PT_PARAMS:
            if key not in params_diff_list and key in pt_params_cnf:
                PT_PARAMS[key] = pt_params_cnf[key]

        if 'units_to_extract' in pt_params_cnf:
            tmp = []
            for unit in pt_params_cnf['units_to_extract']:
                tmp.append(globals()[unit])
            PT_PARAMS['units_to_extract'] = tmp

    if METHOD != 'RMT':
        OVERRIDE = False if DO_REMAINING else OVERRIDE
    else:  # METHOD == 'RMT'
        GOLDEN_ROWS = False

    OUTPUT_DIR = OUTPUT_PATH + f"{METHOD}_{DATASET}_{'GL' if GOLDEN_ROWS else 'RM'}/"
    OUTPUT_FILE = OUTPUT_DIR + '_res.csv'

    NUM_PROCESSORS = multiprocessing.cpu_count() // 2 if NUM_PROCESSORS == 0 else NUM_PROCESSORS

    if OVERRIDE:
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)

    if METHOD == 'PT':
        get_pattern('run_pattern', PT_PARAMS, verbose=False)
    elif METHOD == 'AJ':
        get_pattern('run_aj', {'subset_size': AJ_SUBSET_SIZE, 'num_subsets': AJ_NUM_SUBSET})
    elif METHOD == 'RMT':
        row_matching_test()
    else:
        raise NotImplementedError()
