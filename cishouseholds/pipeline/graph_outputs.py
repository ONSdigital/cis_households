from datetime import datetime
from io import BytesIO

from cishouseholds.hdfs_utils import write_string_to_file


def create_chart(table_operations: dict, output_directory: str):
    import matplotlib
    import networkx as nx
    import json

    bytes_data = json.dumps(table_operations, indent=2).encode("utf-8")
    write_string_to_file(
        bytes_data, f"{output_directory}/process_map-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    )
    matplotlib.use("AGG")
    from matplotlib import pyplot as plt  # noqa: E402

    for stage_name, io in table_operations.items():
        outputs = []
        output_matched = None
        for output in io["outputs"]:
            for ref_name, ref_io in table_operations.items():
                inputs = []
                for input in ref_io["inputs"]:
                    if input == output:
                        inputs.append(stage_name)
                        output_matched = ref_name
                    else:
                        inputs.append(input)
                table_operations[ref_name]["inputs"] = inputs
            if output_matched is not None:
                outputs.append(output_matched)
            else:
                outputs.append(output)
        table_operations[stage_name]["outputs"] = outputs

    _from = []
    _to = []

    for stage_name, io in table_operations.items():
        for output in io["outputs"]:
            _from.append(stage_name)
            _to.append(output)
        for input in io["inputs"]:
            _from.append(input)
            _to.append(stage_name)

    plt.rcParams["figure.figsize"] = [30, 15]
    plt.rcParams["figure.autolayout"] = True

    edges = []
    for from_item, to_item in zip(_from, _to):
        edges.append((from_item, to_item))

    G = nx.DiGraph()
    G.add_edges_from(edges)
    nx.draw_networkx(G, with_labels=True, node_size=100, alpha=1, linewidths=10, arrows=True, arrowsize=20)

    byte_io = BytesIO()
    plt.savefig(byte_io, format="png")
    write_string_to_file(
        byte_io.getbuffer(), f"{output_directory}/process_map-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png"
    )
