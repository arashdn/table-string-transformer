import math
import sys

# COL OUTPUT FORMAT:
'''
res = 
{
    'bidi': True,
    'items': 
    [
        {
            'src_table': 'src_us cities',
            'src_row': 'United States Cities',
            'src_row_id': 0,
            'target_table': 'target_us cities',
            'target_row': 'City',
            'target_row_id': 0
        },
        ...
    ]
}
'''

# ROW OUTPUT FORMAT:
'''
{
    'Kansas City Mo.': ['New York City', 'Salt Lake City', 'Mexico City'],
    ...
}
'''


def get_qgrams(q,s):
    res = set()
    assert q > 0
    if q > len(s):
        return res
    if q == len(s):
        res.add(s)
        return res

    end = len(s) - q + 1

    for i in range(end):
        res.add(s[i:i+q])

    return res


def get_count_matching_q_grams(q, src_set, target_set):
    cnt = 0
    for src in src_set:
        src_qgrams = get_qgrams(q, src)
        for t in target_set:
            t_qgrams = get_qgrams(q, t)
            if not src_qgrams.isdisjoint(t_qgrams):
                cnt += 1
                break

    return cnt


def get_matching_tables(all_tables, tables, q=5, matching_ratio=0.5, src_keys_ratio=0.05, tgt_keys_ratio=0.05, verbose=False):
    prog_cnt = 0
    res = []
    for src in all_tables:
        prog_cnt += 1
        num_src_cols = len(src['titles'])
        if not verbose:
            print('                                                                              ')
            sys.stdout.write("\033[F")
        print(f"Col Matching ({prog_cnt}/{len(all_tables)}) for {src['name']} ({num_src_cols} cols)")
        if not verbose:
            sys.stdout.write("\033[F")

        # if src['name'] == "src_new york govs 1":
        #     pass
        for i in range(num_src_cols):
            col_src = [src['items'][j][i].lower() for j in range(len(src['items']))]
            dup_src = len(col_src) - len(set(col_src))
            if dup_src > math.ceil(src_keys_ratio * len(col_src)):
                continue
            for tgt in all_tables:
                if tgt['name'] == src['name']:
                    continue
                num_tgt_cols = len(tgt['titles'])
                for k in range(num_tgt_cols):
                    col_tgt = [tgt['items'][j][k].lower() for j in range(len(tgt['items']))]
                    dup_tgt = len(col_tgt) - len(set(col_tgt))


                    if verbose and tgt['name'].split('_')[1] == src['name'].split('_')[1] \
                            and tables[tgt['name'].split('_')[1]]['rows']['src'] == src['titles'][i] \
                            and tables[tgt['name'].split('_')[1]]['rows']['target'] == tgt['titles'][k]:
                        # print("*****INFO********")
                        print(f"^^^^^{src['name']}[{src['titles'][i]}] --> {tgt['name']}[{tgt['titles'][k]}] **" +
                              f" Dup src:{dup_src}/{len(col_src)}, Dup tgt:{dup_tgt}/{len(col_tgt)} ** " +
                              f"Score: {get_count_matching_q_grams(q, col_src, col_tgt)} / {len(col_src)}" +
                              f" (~{get_count_matching_q_grams(q, col_src, col_tgt)*100//len(col_src)}%)")
                        # print("**************")

                    if dup_tgt > math.ceil(tgt_keys_ratio * len(col_tgt)):
                        continue

                    cnt = get_count_matching_q_grams(q, col_src, col_tgt)
                    if cnt > len(col_src) * matching_ratio:
                        # print(f"{cnt} / {len(col_src)}")
                        # print(f"Dup src:{dup_src}/{len(col_src)}, Dup tgt:{dup_tgt}/{len(col_tgt)}")
                        if verbose:
                            print(f"{src['name']}[{src['titles'][i]}] --> {tgt['name']}[{tgt['titles'][k]}] **"+
                                  f"Dup src:{dup_src}/{len(col_src)}, Dup tgt:{dup_tgt}/{len(col_tgt)} ** "+
                                  f"Score: {cnt} / {len(col_src)} (~{cnt*100//len(col_src)}%)")
                        res.append({
                            'src_table': src['name'],
                            'src_row': src['titles'][i],
                            'src_row_id': i,
                            'target_table': tgt['name'],
                            'target_row': tgt['titles'][k],
                            'target_row_id': k,
                        })

    return {
        'items': res,
        'bidi': True,
    }


#Cols: a single item in col output.items:
def get_matching_rows_for_table(tables, cols, q_start=3, q_end=16, swap_src_target=False, fast_mode=False):
    assert cols['src_table'].startswith('src') or cols['src_table'].startswith('target')
    assert cols['target_table'].startswith('src') or cols['target_table'].startswith('target')

    src_pre, src_name = cols['src_table'].split('_', 1)
    target_pre, target_name = cols['target_table'].split('_', 1)

    src_tbl = tables[src_name][src_pre]
    target_tbl = tables[target_name][target_pre]

    src, target = set(), set()

    for item in src_tbl['items']:
        if fast_mode or item[cols['src_row_id']] not in src:
            src.add(item[cols['src_row_id']])

    for item in target_tbl['items']:
        if fast_mode or item[cols['target_row_id']] not in target:
            target.add(item[cols['target_row_id']])

    src = list(src)
    target = list(target)

    tmp, is_swapped = get_matching_rows(src, target, q_start, q_end, swap_src_target)

    new_res = tmp
    if fast_mode:
        new_res = {}
        for item in tmp:
            t = set(tmp[item])
            new_res[item] = list(t)

    return new_res, is_swapped


def get_matching_rows(src, target, q_start=3, q_end=16, swap_src_target=False):
    is_swapped = False
    if swap_src_target:
        avg_src = sum(len(c) for c in src) / len(src)
        avg_target = sum(len(c) for c in target) / len(target)
        # print(f"src={avg_src}, target={avg_target}")
        if avg_target > avg_src:
            src, target = target, src
            is_swapped = True

    tmp = get_matching_rows_by_list(src, target, q_start, q_end)

    return tmp, is_swapped


# Get matching rows from two lists
def get_matching_rows_by_list(col_src, col_target, q_start, q_end):
    src_qgrams = {}
    target_qgrams = {}
    # {'gram':[r1, r2, ...]}

    # building index:
    for q in range(q_start, q_end):
        for i, s in enumerate(col_src):
            grams = get_qgrams(q, s)
            for gram in grams:
                if gram in src_qgrams:
                    src_qgrams[gram].append(i)
                else:
                    src_qgrams[gram] = [i]

        for i, s in enumerate(col_target):
            grams = get_qgrams(q, s)
            for gram in grams:
                if gram in target_qgrams:
                    target_qgrams[gram].append(i)
                else:
                    target_qgrams[gram] = [i]

    res = {}
    cnt_fail = 0
    for s in col_src:
        best_gram, best_score = None, -1
        for q in range(q_start, q_end):
            src_grams = get_qgrams(q, s)
            for gram in src_grams:
                if gram in src_qgrams and gram in target_qgrams:
                    n = len(src_qgrams[gram])
                    m = len(target_qgrams[gram])
                    score = 1/(n*m)
                    if score > best_score:
                        best_score = score
                        best_gram = gram

        try:
            res[s] = [col_target[i] for i in target_qgrams[best_gram]]
        except KeyError:
            cnt_fail += 1

    print(f"  {cnt_fail}/{len(col_src)} rows with no match")
    return res




def get_matching_rows_golden(tables, cols, swap_src_target=False):
    assert cols['src_table'].startswith('src') or cols['src_table'].startswith('target')
    assert cols['target_table'].startswith('src') or cols['target_table'].startswith('target')

    src_type, src_tbl = cols['src_table'].split('_', 1)
    target_type, target_tbl = cols['target_table'].split('_', 1)

    src_row = cols['src_row']
    target_row = cols['target_row']


    is_swapped = False
    if swap_src_target:
        src_tab = tables[src_tbl][src_type]
        target_tab = tables[target_tbl][target_type]

        src, target = set(), set()
        for item in src_tab['items']:
            if item[cols['src_row_id']] not in src:
                src.add(item[cols['src_row_id']])

        for item in target_tab['items']:
            if item[cols['target_row_id']] not in target:
                target.add(item[cols['target_row_id']])

        src = list(src)
        target = list(target)

        avg_src = sum(len(c) for c in src) / len(src)
        avg_target = sum(len(c) for c in target) / len(target)
        # print(f"src={avg_src}, target={avg_target}")
        if avg_target > avg_src:
            src, target = target, src
            src_type, target_type = target_type, src_type
            src_tbl, target_tbl = target_tbl, src_tbl
            src_row, target_row = target_row, src_row
            is_swapped = True

    src_pref = 'source' if src_type == 'src' else 'target'
    target_pref = 'source' if target_type == 'src' else 'target'
    gt_src_col = src_pref + "-" + src_row
    gt_target_col = target_pref + "-" + target_row

    gt_info = tables[src_tbl]['GT']
    gt_src_row_id = gt_info['titles'].index(gt_src_col)
    gt_target_row_id = gt_info['titles'].index(gt_target_col)

    gt = {}
    for item in gt_info['items']:
        if item[gt_src_row_id] in gt:
            gt[item[gt_src_row_id]].add(item[gt_target_row_id])
        else:
            gt[item[gt_src_row_id]] = {item[gt_target_row_id], }

    return gt, is_swapped



def get_matching_tables_golden(tables, bidi=False):
    res = []
    for name, item in tables.items():
        i = item['src']['titles'].index(item['rows']['src'])
        k = item['target']['titles'].index(item['rows']['target'])
        res.append({
            'src_table': item['src']['name'],
            'src_row': item['src']['titles'][i],
            'src_row_id': i,
            'target_table': item['target']['name'],
            'target_row': item['target']['titles'][k],
            'target_row_id': k,
        })
        if bidi:
            res.append({
                'src_table': item['target']['name'],
                'src_row': item['target']['titles'][k],
                'src_row_id': k,
                'target_table': item['src']['name'],
                'target_row': item['src']['titles'][i],
                'target_row_id': i,
            })

    return {
        'items': res,
        'bidi': bidi,
    }


# Just to test effectivity of considering the longer columns as the source one
def get_correct_source_target_cols(tables):
    import Matcher as matcher
    res = matcher.get_matching_tables_golden(tables, bidi=False)

    swap_cnt = 0
    cnt = 0
    wrongs = []
    for item in res['items']:
        print(f"Matching rows for '{item['src_table']}' ...")
        rows, is_swapped = matcher.get_matching_rows_golden(tables, item, swap_src_target=True)

        tbl = item['src_table'][4:]
        src_col = tables[tbl]['source_col']

        need_swap = src_col == 'target'

        if (need_swap and not is_swapped) or (not need_swap and is_swapped):
            wrongs.append(tbl)

        cnt += 1
        if is_swapped:
            swap_cnt += 1
            print("swapped")

    print(f"total swaps = {swap_cnt}/{cnt}")
    print(f"Wrongs: {len(wrongs)}")
    print(wrongs)
