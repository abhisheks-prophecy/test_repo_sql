from setuptools import setup, find_packages
setup(
    name = 'StreamingTestPipeline_1Clone',
    version = '1.0',
    packages = find_packages(include = ('streamingtestpipeline_1*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.11.10'],
    entry_points = {
'console_scripts' : [
'main = streamingtestpipeline_1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
