import os
import sys


# OUTPUT FORMAT
'''
all_tables:
[
    {
        'name': 'src_california govs 1', 
        'titles': ["Governor's Name", 'State', 'Time in Office', 'Party'], 
        'items': [
                    ['Gov. Arnold Schwarzenegger', 'California', '(2003 - 2011)', 'Republican'], 
                    ['Gov. Gray Davis', 'California', '(1999 - 2003)', 'Democrat'], 
                    ['Gov. Pete Wilson', 'California', '(1991 - 1999)', 'Republican'], 
                  ]
    }, 
    {
        'name': 'target_california govs 1', 
        'titles': ['Name', 'Lifespan', 'Political Party', 'Years of Service', 'Native State', 'Notes'], 
        'items': [
                    ['Peter H. Burnett', '1807 - 1895', 'Democrat', '1849 - 1851', 'Tennessee', '(16)'], 
                    ['John McDougal', '1818 - 1866', 'Democrat', '1851 - 1852', 'Ohio', ''], 
                    ['John Bigler', '1805 - 1871', 'Democrat', '1852 - 1856', 'Pennsylvania', ''],
                  ]
    }
]
'''
'''
tables:
{
    'california govs 1': 
    {
        'src': 
        {
            'name': 'src_california govs 1', 
            'titles': ["Governor's Name", 'State', 'Time in Office', 'Party'], 
            'items': 
            [
                ['Gov. Arnold Schwarzenegger', 'California', '(2003 - 2011)', 'Republican'], 
                ['Gov. Gray Davis', 'California', '(1999 - 2003)', 'Democrat'], 
                ['Gov. Pete Wilson', 'California', '(1991 - 1999)', 'Republican'],
            ]
        }, 
        'target': 
        {
            'name': 'target_california govs 1', 
            'titles': ['Name', 'Lifespan', 'Political Party', 'Years of Service', 'Native State', 'Notes'],
            'items': 
            [
                ['Peter H. Burnett', '1807 - 1895', 'Democrat', '1849 - 1851', 'Tennessee', '(16)'], 
                ['John McDougal', '1818 - 1866', 'Democrat', '1851 - 1852', 'Ohio', ''], 
                ['John Bigler', '1805 - 1871', 'Democrat', '1852 - 1856', 'Pennsylvania', ''],
            ]
        },
        'name': 'california govs 1', 
        'rows': {'src': "Governor's Name", 'target': 'Name'},
        'source_col': source
    }, 
}         
'''


def get_tables_from_dir(ds_path, tbl_names, make_lower=False, verbose=False):
    tables = {}
    all_tables = []
    assert os.path.isdir(ds_path)
    dirs = [dI for dI in os.listdir(ds_path) if os.path.isdir(os.path.join(ds_path, dI))]
    for dir in dirs:
        if len(tbl_names) > 0 and dir not in tbl_names:
            if verbose: print(f"*** {dir} not in specified names")
            continue
        if '_' in dir:
            raise Exception("_ cannot be in dir name: "+dir)
        if verbose: print("Reading "+dir)
        ds_dir = ds_path+'/' + dir
        assert os.path.exists(ds_dir + "/source.csv")
        assert os.path.exists(ds_dir + "/target.csv")
        assert os.path.exists(ds_dir + "/rows.txt")

        has_gt = os.path.exists(ds_dir + "/ground truth.csv")

        res = {
            'src': {'name': 'src_'+dir, 'titles': None, 'items': []},
            'target': {'name': 'target_'+dir, 'titles': None, 'items': []},
            'name': dir
        }

        if has_gt:
            res['GT'] = {'titles': None, 'items': []}


        with open(ds_dir + "/source.csv") as f:
            res['src']['titles'] = f.readline().strip().split(',')
            if make_lower:
                res['src']['items'] = [line.lower().strip().split(',') for line in f.readlines()]
            else:
                res['src']['items'] = [line.strip().split(',') for line in f.readlines()]

        with open(ds_dir + "/target.csv") as f:
            res['target']['titles'] = f.readline().strip().split(',')
            if make_lower:
                res['target']['items'] = [line.lower().strip().split(',') for line in f.readlines()]
            else:
                res['target']['items'] = [line.strip().split(',') for line in f.readlines()]

        if has_gt:
            with open(ds_dir + "/ground truth.csv") as f:
                res['GT']['titles'] = f.readline().strip().split(',')
                if make_lower:
                    res['GT']['items'] = [line.lower().strip().split(',') for line in f.readlines()]
                else:
                    res['GT']['items'] = [line.strip().split(',') for line in f.readlines()]

        with open(ds_dir + "/rows.txt") as f:
            l = f.readline().strip().split(':')
            s = l[0]
            t = l[1]
            assert s in res['src']['titles']
            assert t in res['target']['titles']
            res['rows'] = {'src': s, 'target': t}

            l = f.readline().strip()
            assert l in ("source", "target")
            res['source_col'] = l

        if has_gt:
            assert len(res['GT']['titles']) == len(res['src']['titles']) + len(res['target']['titles'])

            change = not res['GT']['titles'][0].startswith("source-")

            for i in range(0, len(res['src']['titles'])):
                if change:
                    res['GT']['titles'][i] = 'source-' + res['GT']['titles'][i]

                assert res['GT']['titles'][i] == 'source-' + res['src']['titles'][i]

            for i in range(len(res['src']['titles']), len(res['GT']['titles'])):
                if change:
                    res['GT']['titles'][i] = 'target-' + res['GT']['titles'][i]

                assert res['GT']['titles'][i] == 'target-' + res['target']['titles'][i-len(res['src']['titles'])]

        tables[dir] = res
        all_tables.append(res['src'])
        all_tables.append(res['target'])
    return tables, all_tables


def basic_loader(ds_path, make_lower=False):
    assert os.path.isdir(ds_path)
    ds_path = os.path.normpath(ds_path)
    import pathlib
    path = str(pathlib.Path(ds_path).absolute().parent)
    dir = os.path.basename(ds_path)
    tables, all_tables = get_tables_from_dir(path, [dir], make_lower, verbose=False)
    tbl = tables[dir]

    src_row_id = tbl['src']['titles'].index(tbl['rows']['src'])
    target_row_id = tbl['target']['titles'].index(tbl['rows']['target'])

    src = []
    target = []

    src_tbl = tbl['src']
    for item in src_tbl['items']:
        src.append(item[src_row_id])

    target_tbl = tbl['target']
    for item in target_tbl['items']:
        target.append(item[target_row_id])


    gt = None
    has_GT = 'GT' in tbl
    if has_GT:
        src_type, src_tbl = tbl['src']['name'].split('_', 1)
        target_type, target_tbl = tbl['target']['name'].split('_', 1)

        gt_src_col = 'source-' + tbl['rows']['src']
        gt_target_col = "target-" + tbl['rows']['target']

        gt_info = tbl['GT']
        gt_src_row_id = gt_info['titles'].index(gt_src_col)
        gt_target_row_id = gt_info['titles'].index(gt_target_col)

        gt = []
        for item in gt_info['items']:
            gt.append((item[gt_src_row_id], item[gt_target_row_id]))

    return src, target, gt
