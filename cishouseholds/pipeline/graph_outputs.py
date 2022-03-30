from io import BytesIO

from cishouseholds.hdfs_utils import write_string_to_file


def create_chart(test: dict, output_directory: str):
    import matplotlib
    import networkx as nx
    import pandas as pd
    import json

    bytes_data = json.dumps(test, indent=2).encode("utf-8")
    write_string_to_file(bytes_data, f"{output_directory}/process_map.txt")
    matplotlib.use("AGG")
    from matplotlib import pyplot as plt  # noqa: E402

    for stage_name, io in test.items():
        outputs = []
        output_matched = None
        for output in io["outputs"]:
            for ref_name, ref_io in test.items():
                inputs = []
                for input in ref_io["inputs"]:
                    if input == output:
                        inputs.append(stage_name)
                        output_matched = ref_name
                    else:
                        inputs.append(input)
                test[ref_name]["inputs"] = inputs
            if output_matched is not None:
                outputs.append(output_matched)
            else:
                outputs.append(output)
        test[stage_name]["outputs"] = outputs

    _from = []
    _to = []

    for stage_name, io in test.items():
        for output in io["outputs"]:
            _from.append(stage_name)
            _to.append(output)
        for input in io["inputs"]:
            _from.append(input)
            _to.append(stage_name)

    plt.rcParams["figure.figsize"] = [15, 7]
    plt.rcParams["figure.autolayout"] = True

    df = pd.DataFrame({"from": _from, "to": _to})
    G = nx.from_pandas_edgelist(df, "from", "to")
    nx.draw(G, with_labels=True, node_size=100, alpha=1, linewidths=10)

    byte_io = BytesIO()
    plt.savefig(byte_io)
    write_string_to_file(byte_io.read(), f"{output_directory}/process_map.png")
