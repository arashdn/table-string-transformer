class RowMatcherUnit:
    def __init__(self, tables, rows):
        self.tables = tables
        self.rows = rows
        self._tp = None
        self._fp = None
        self._fn = None
        self._init()

    def _init(self):
        src_type, src_tbl = self.rows['col_info']['src_table'].split('_', 1)
        target_type, target_tbl = self.rows['col_info']['target_table'].split('_', 1)
        src_row = self.rows['col_info']['src_row']
        target_row = self.rows['col_info']['target_row']
        # src_row_id = self.rows['col_info']['src_row_id']
        # target_row_id = self.rows['col_info']['target_row_id']

        if self.rows['is_swapped']:
            src_type, target_type = target_type, src_type
            src_tbl, target_tbl = target_tbl, src_tbl
            src_row, target_row = target_row, src_row
            # src_row_id, target_row_id = target_row_id, src_row_id

        src_pref = 'source' if src_type == 'src' else 'target'
        target_pref = 'source' if target_type == 'src' else 'target'
        gt_src_col = src_pref + "-" + src_row
        gt_target_col = target_pref + "-" + target_row

        gt_info = self.tables[src_tbl]['GT']
        gt_src_row_id = gt_info['titles'].index(gt_src_col)
        gt_target_row_id = gt_info['titles'].index(gt_target_col)

        gt = {}
        fn = 0
        for item in gt_info['items']:
            if item[gt_src_row_id] in gt:
                gt[item[gt_src_row_id]].add(item[gt_target_row_id])
            else:
                gt[item[gt_src_row_id]] = {item[gt_target_row_id], }

            if item[gt_src_row_id] not in self.rows['rows']:
                fn += 1
            else:
                rows = [x[0] for x in self.rows['rows'][item[gt_src_row_id]]]
                if item[gt_target_row_id] not in rows:
                    fn += 1

        self._fn = fn
        tp, fp = 0, 0
        for s, val in self.rows['rows'].items():
            if s not in gt.keys():
                fp += 1
                continue
            for tg in val:
                if tg[0] in gt[s]:
                    tp += 1
                else:
                    fp += 1

        self._tp = tp
        self._fp = fp

    @property
    def tp(self):
        return self._tp

    @property
    def fp(self):
        return self._fp

    @property
    def fn(self):
        return self._fn


    @property
    def precision(self):
        return 0.0 if self.tp == 0 else self.tp/(self.tp + self.fp)

    @property
    def recall(self):
        return 0.0 if self.tp == 0 else self.tp / (self.tp + self.fn)

    @property
    def f1(self):
        if self.precision + self.recall == 0:
            return 0.0
        return (2 * self.precision * self.recall) / (self.precision + self.recall)

    def __str__(self):
        return f"({self.rows['col_info']['src_table'].split('_', 1)[1]}) -> TP:{self.tp}, FP={self.fp}, FN={self.fn}, P={self.precision}, R={self.recall}, F1={self.f1}"


class RowMatcherEval:
    def __init__(self, tables, all_rows):
        self.units = [RowMatcherUnit(tables, rows) for rows in all_rows]
        self.n = len(self.units)

    @property
    def precision(self):
        return sum(unit.precision for unit in self.units) / self.n

    @property
    def recall(self):
        return sum(unit.recall for unit in self.units) / self.n

    @property
    def f1(self):
        return sum(unit.f1 for unit in self.units) / self.n

    def __str__(self):
        s = ""
        # for unit in self.units:
        #     s += "\n  " + str(unit)
        return f"P={self.precision}, R={self.recall}, F1={self.f1}" + s

'''
P: correct_gen/generated
R: correct_gen/golden output
Micro: calculate P and R for each inp and avg them
Macro: cumulative numerator and denumerator
E.g.: 
----
inp: Arash Dargahi
golden output: adargahi, dargahi
gen output: dargahi, arash, addd, adar
P: 1/4, R: 1/2
'''
'''
import heapq

class TransformerEval:
    def __init__(self, final_list, rows, time):
        self.final_list = final_list
        self.rows = rows
        self.time = time

    def gain(self, n):  # average gain of n transformation
        temp = heapq.nlargest(n, self.final_list)
        return sum(t.gain for t in temp) / n

    def precision(self, n):  # micro, macro are the same
        temp = heapq.nlargest(n, self.final_list)
        sum_micro, sum_macro, cnt_macro = 0, 0, 0

        for inp, outss in self.rows['rows'].items():
            cor = 0
            cnt = 0
            outs = [o[0] for o in outss]
            for item in temp:
                cnt += 1
                gen = tr.apply_op_list(inp, item.trans)
                if gen in outs:
                    cor += 1

            sum_micro += cor/cnt

            sum_macro += cor
            cnt_macro += cnt

        return sum_micro / len(self.rows['rows'].items())  # , sum_macro / cnt_macro

    def recall(self, n):  # micro, macro are the same
        temp = heapq.nlargest(n, self.final_list)
        sum_micro, sum_macro, cnt_macro = 0, 0, 0

        for inp, outss in self.rows['rows'].items():
            cor = 0
            outs = [o[0] for o in outss]
            gens = []
            for item in temp:
                gen = tr.apply_op_list(inp, item.trans)
                gens.append(gen)
            for out in outs:
                if out in gens:
                    cor += 1

            sum_micro += cor / len(outs)

            sum_macro += cor
            cnt_macro += len(outs)

        return sum_micro / len(self.rows['rows'].items())  # , sum_macro / cnt_macro

    @classmethod
    def text(cls):
        return "Table,time,P@1,P@2,P@3,P@4,P@5,P@6,P@7,P@8,P@9,P@10," + \
               "R@1,R@2,R@3,R@4,R@5,R@6,R@7,R@8,R@9,R@10," + \
               "G@1,G@2,G@3,G@4,G@5,G@6,G@7,G@8,G@9,G@10"

    def __str__(self):

        return f"{self.rows['col_info']['src_table']},{self.time}," + \
               f"{self.precision(1)},{self.precision(2)},{self.precision(3)}," + \
               f"{self.precision(4)},{self.precision(5)},{self.precision(6)}," + \
               f"{self.precision(7)},{self.precision(8)},{self.precision(9)},{self.precision(10)}," + \
               f"{self.recall(1)},{self.recall(2)},{self.recall(3)}," + \
               f"{self.recall(4)},{self.recall(5)},{self.recall(6)}," + \
               f"{self.recall(7)},{self.recall(8)},{self.recall(9)},{self.recall(10)}," + \
               f"{self.gain(1)},{self.gain(2)},{self.gain(3)}," + \
               f"{self.gain(4)},{self.gain(5)},{self.gain(6)}," + \
               f"{self.gain(7)},{self.gain(8)},{self.gain(9)},{self.gain(10)}"
'''