from setuptools import setup, find_packages
setup(
    name = 'StreamingWorking',
    version = '1.0',
    packages = find_packages(include = ('streamingpipelinemain*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.19'],
    entry_points = {
'console_scripts' : [
'main = streamingpipelinemain.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)