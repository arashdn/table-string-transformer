import pathlib

BASE_PATH = str(pathlib.Path(__file__).absolute().parent.parent.parent.absolute())
PATH = BASE_PATH + "/output/PT_BM_GL/fsu name to username.txt"


def brief_print(final_res, all_rows_cnt, unique_row_cnt):
    s = ""
    cnt = 0
    for tr in final_res:
        cnt += 1
        s += str(cnt) + ": {" + f"id: {tr['id']}\n" + \
        f"   tran: {tr['trans']}\n" + \
        f"   new coverage: {tr['new_coverage']}/{unique_row_cnt} = {(tr['new_coverage']/unique_row_cnt)*100}%,   original coverage:{tr['coverage']}\n" + \
        f"   cumulative coverage: {tr['cumulative_cov']} = {(tr['cumulative_cov']/unique_row_cnt)*100}%\n" + "}\n"

    s += f"Total unique input rows: {unique_row_cnt}/{all_rows_cnt}"

    return s



def main():
    final_res = []
    all_inp = set()
    tr_id = 1


    init_str = "pattern list: {"
    cov_text = "Coverage: "
    with open(PATH, 'r') as f:
        for line in f.readlines():
            if "golden pattern list: {" in line:
                init_str = "golden pattern list: {"
                cov_text = "Coverage in golden matched rows: "
                print("Golden patterns in file.")
                break


    with open(PATH, 'r') as f:
        line = ""
        all_rows_cnt = None
        while not line.startswith(init_str):
            if line.startswith(cov_text):
                num = line.strip().split(': ')[1].split(" = ")[0]
                all_rows_cnt = int(num.split("/")[1])
            line = f.readline()

        line = f.readline().strip()

        while line != "":
            assert line.startswith("{[")
            ll = line.strip().split("  -- ")[0].split(" -> ")
            assert len(ll) == 2
            tr = {
                'id': tr_id,
                'trans': ll[0],
                'coverage': int(ll[1].split("/")[0]),
                'rows': set(),
                'new_rows': set(),
            }
            tr_id += 1

            line = f.readline().strip()
            while line.startswith('|->'):

                splitter = "\'"
                if len(line.split("\', \'")) != 2:
                    splitter = '"'

                ll = line.strip().split(f"{splitter}, {splitter}")
                assert len(ll) == 2

                tmp_inp = ll[0].split(f"|->({splitter}")
                assert len(tmp_inp) == 2
                inp = tmp_inp[1]

                tmp_out = ll[1].split(f"{splitter})")
                assert len(tmp_out) == 2
                out = tmp_out[0]

                row = (inp, out)
                tr['rows'].add(row)

                if row not in all_inp:
                    tr['new_rows'].add(row)
                    all_inp.add(row)

                line = f.readline().strip()

            assert tr['coverage'] == len(tr['rows'])
            tr['new_coverage'] = len(tr['new_rows'])

            final_res.append(tr)

        if init_str == "golden pattern list: {":
            line = f.readline()
            assert "--- Not covered inputs:" in line
            line = f.readline()
            while line.strip() != "****************":
                splitter = "\'"
                if len(line.split("\', \'")) != 2:
                    splitter = '"'

                ll = line.strip().split(f"{splitter}, {splitter}")
                assert len(ll) == 2

                tmp_inp = ll[0].split(f"({splitter}")
                assert len(tmp_inp) == 2
                inp = tmp_inp[1]

                tmp_out = ll[1].split(f"{splitter})")
                assert len(tmp_out) == 2
                out = tmp_out[0]

                row = (inp, out)

                if row not in all_inp:
                    all_inp.add(row)

                line = f.readline()

    final_res.sort(key=lambda x: x['new_coverage'], reverse=True)
    cov = 0
    for key, tr in enumerate(final_res):
        cov += tr['new_coverage']
        final_res[key]['cumulative_cov'] = cov



    s = brief_print(final_res, all_rows_cnt, len(all_inp))
    print(s)



if __name__ == "__main__":
    print(BASE_PATH)
    main()
