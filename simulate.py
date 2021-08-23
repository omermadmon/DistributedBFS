import random
from itertools import permutations
from vertex import vertex
from threading import Thread


def build_graph(graph_size, pick_edge_prob, tcp_port, increment):
    graph = {}
    vertices = []
    for i in range(1, graph_size + 1):
        tcp_port += increment
        ID = i
        num = random.randint(0, graph_size * graph_size)
        graph[ID] = {'neighbors': [], 'TCP_port': tcp_port, 'input_number': num}
        vertices.append(ID)
    edges = list(permutations(vertices, 2))
    random.shuffle(edges)
    for edge in edges:
        u = edge[0]
        v = edge[1]
        if u not in graph[v]['neighbors']:
            prob = random.random()
            if prob < pick_edge_prob:
                graph[u]['neighbors'].append(v)
                graph[v]['neighbors'].append(u)
    return graph


def is_connected(graph, graph_size):
    reachable_vertices = list(graph[1]['neighbors'])
    reachable_vertices.append(1)
    unreachable_vertices = [item for item in range(1, graph_size + 1) if item not in reachable_vertices]
    threshold = graph_size
    while len(unreachable_vertices) != threshold and len(unreachable_vertices) != 0:
        threshold = len(unreachable_vertices)
        for v in reachable_vertices:
            for neighbor in graph[v]['neighbors']:
                if neighbor in unreachable_vertices:
                    reachable_vertices.append(neighbor)
                    unreachable_vertices.remove(neighbor)
    if len(unreachable_vertices) == 0:
        return True
    return False


def vertices_input(graph):
    for v in graph:
        tcp_port = graph[v]['TCP_port']
        num = graph[v]['input_number']
        neighbors = list(graph[v]['neighbors'])
        input_file_name = 'input_vertex_' + str(v) + '.txt'
        input_file = open(input_file_name, 'w')
        input_file.write(str(len(graph)) + '\n')
        input_file.write(str(tcp_port) + '\n')
        input_file.write(str(num) + '\n')
        for neighbor in neighbors:
            input_file.write(str(graph[neighbor]['TCP_port']) + '\n')
            input_file.write('127.0.0.1\n')
        input_file.write('*\n')
        input_file.close()


def get_tree_from_output(ids):
    t = {}
    for ID in ids:
        t[ID] = {'children': []}
        with open('output_vertex_' + str(ID) + '.txt', 'r') as file:
            t[ID]['distance'] = int(file.readline())
            line = file.readline()
            if ID != 1:
                t[ID]['parent'] = int(line)
            else:
                t[ID]['parent'] = None
            t[ID]['sum'] = int(file.readline())
    for ID in ids:
        if t[ID]['parent'] != None:
            parent = t[ID]['parent']
            t[parent]['children'].append(ID)
    return t


def check_bfs_correctness(graph, tree):
    for ID in graph.keys():
        if tree[ID]['parent'] is not None:
            parent = tree[ID]['parent']
            if parent not in graph[ID]['neighbors']:
                return False, 'Tree error: edge (' + str(ID) + ',' + str(parent) + ') not in the graph'
            min_dist = len(graph) + 1
            for neighbor in graph[ID]['neighbors']:
                d = tree[neighbor]['distance']
                if min_dist > d:
                    min_dist = d
            if min_dist != tree[parent]['distance'] or tree[ID]['distance'] != 1 + min_dist:
                return False, 'Distance error: vertex ' + str(ID)
        elif ID != 1 or tree[ID]['distance'] != 0:
            return False, 'Root error'
    return True, 'BFS is correct'


def check_sum_correctness(tree, graph):
    for ID in tree.keys():
        subtree_sum = tree[ID]['sum']
        children = list(tree[ID]['children'])
        sum_of_children = 0
        for child in children:
            sum_of_children += tree[child]['sum']
        if subtree_sum != graph[ID]['input_number'] + sum_of_children:
            return False, 'Sum error: vertex ' + str(ID)
    return True, 'Sum is correct'


def main():
    # constructs and simulates a graph.
    try:
        graph_size = int(input('Enter graph size: '))
    except:
        print('ERROR--graph size')
    random.seed(45990)
    pick_edge_prob = 0.2
    tcp_port_start = 41000
    increment = 1
    graph_connectivity = False
    while not graph_connectivity:
        graph = build_graph(graph_size, pick_edge_prob, tcp_port_start, increment)
        graph_connectivity = is_connected(graph, graph_size)
    vertices_input(graph)
    threads = []
    ids = list(graph.keys())
    for ID in ids:
        threads.append(Thread(target=vertex, args=(ID,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # checks correctness
    tree = get_tree_from_output(ids)
    bfs_flag, bfs_comment = check_bfs_correctness(graph, tree)
    sum_flag, sum_comment = check_sum_correctness(tree, graph)
    print(bfs_comment)
    print(sum_comment)

    return graph


if __name__ == '__main__':
    main()
