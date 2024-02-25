from setuptools import find_packages,setup
from typing import List

HYPEN_E_DOT='-e .'
def get_requirements(file_path:str)->List[str]:
    '''
    This function takes a file path as input and returns a list of requirements.

    Parameters:
    - file_path (str): The path to the file containing the requirements.

    Returns:
    - List[str]: A list of requirements extracted from the specified file.

    The function reads the contents of the file specified by 'file_path' and extracts
    the requirements, assuming each requirement is represented as a separate line in the file.
    It removes any newline characters from the extracted lines. Additionally, if the string
    'HYPEN_E_DOT' is present in the list of requirements, it is removed.

    Example Usage:
    ```
    file_path = "path/to/requirements.txt"
    requirements_list = get_requirements(file_path)
    print(requirements_list)
    ```
    '''
    requirements=[]
    with open(file_path) as file_obj:
        # Read lines from the file and remove newline characters
        requirements=file_obj.readlines()

        # Remove 'HYPEN_E_DOT' from the requirements if present
        requirements=[req.replace("\n","") for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    
    # Return the list of requirements
    return requirements

setup(
name='geopipeline',
version='0.0.1',
author='BVSh',
author_email='beehzad.valipour@outlook.com',
packages=find_packages(),
install_requires=get_requirements('requirements.txt')
)
# TODO: I should create specific email for my github to share for the public