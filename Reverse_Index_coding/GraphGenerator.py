from graphviz import Digraph

class GraphGenerator(object):

    def draw_graph(self, graph, reducer_id, uci_links):
        g_all = Digraph('unix', filename='out_graph_all_' + str(reducer_id) +'.gv')
        g_all.attr(size='6,6')
        for entry in graph.keys():
            for tar in graph[entry]:
                g_all.edge(str(tar), str(entry))

        g_all.view()

        g_uci = Digraph('unix', filename='out_graph_uci_' + str(reducer_id) + '.gv')
        g_uci.attr(size='6,6')
        for entry in graph.keys():
            if entry not in uci_links:
                continue
            for tar in graph[entry]:
                if tar not in uci_links:
                    continue
                g_uci.edge(str(tar), str(entry))

        g_uci.view()

        return


if __name__ == "__main__":
    g = GraphGenerator([(2,1,3), (3,2,1), (3,5,6)])


    g.draw_graph()

