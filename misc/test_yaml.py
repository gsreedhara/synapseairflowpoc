import yaml

with open('/Users/giridharsreedhara/Library/CloudStorage/OneDrive-ProjectXLtd/My Documents/pythonprojects/git repos/synapseairflowpoc/business_critical_pipelines_batch.yml') as file:
    try:
        data = yaml.safe_load(file)
        print(type(data))
        print(data)
        for pipeline in data['pipelines']:
            print(pipeline)
    except yaml.YAMLError as exception:
        print(exception)