import inspect,os
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.pipeline_stages import pipeline_stages

def generate_config():
    current_config = get_config()["stages"]
    config_str = ""
    for stage_name,stage_function in pipeline_stages.items():
        config_str += f"- function {stage_name}\n   run: False\n"
        inspection = inspect.getfullargspec(stage_function)
        for arg in inspection.args:
            #print(inspect.signature(stage_function).parameters[arg])
            if "=" in str(inspect.signature(stage_function).parameters[arg]):
                config_str += f"   #{arg}: "
            else:
                config_str += f"   {arg}: "
            if arg in current_config.get(stage_name,{}) and arg is not None:
                print("$$$",current_config[stage_name][arg])
                if type(current_config[stage_name][arg]) == list:
                    for val in current_config[stage_name][arg]:
                        config_str += f"    - {val}\n"
                else:
                    config_str += current_config[stage_name][arg]
            config_str += "\n"
        config_str += "\n"
    print(config_str)
    with open(os.environ.get("PIPELINE_CONFIG_LOCATION"),"w+") as fh:
        fh.write(config_str)
generate_config()

# def write_lines(config,config_str):
#     if type(config) == str:
                    
                    
#                     for val in current_config[stage_name][arg]:
#                         config_str += f"    - {val}\n"
#                 else:
#                     config_str += current_config[stage_name][arg]