from setuptools import setup, find_packages
setup(
    name = 'PERF_PY_PIP',
    version = '1.0',
    packages = find_packages(include = ('com.main1.pythondepmanagement_1*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = ['matplotlib==3.5.2', 'pandas>=1.4.2', 'numpy==1.22.*', 'requests~=2.28.0', 'scipy>=1.6.3,<=1.8.1', 'torch==1.11.0',
     'prophecy-libs==1.3.12'],
    entry_points = {
'console_scripts' : [
'main = com.main1.pythondepmanagement_1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)