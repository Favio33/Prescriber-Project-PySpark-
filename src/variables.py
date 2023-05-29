import os

# Set Environment Variables
os.environ['env'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

# Ger Environment Variables
env = os.environ['env']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

# Set Other Variables
appName = "USA Prescriber Research Report"
currentPath = os.getcwd()
pathStagingDimensionCity = currentPath + '\\resources\\staging\\dimension_city'
pathStagingFact = currentPath + '\\resources\\staging\\fact'

# Output paths
output_city = "PrescPipeline/output/dimension_city"
output_fact = "PrescPipeline/output/presc"
