#!/usr/bin/env python
import click
import multiprocessing
from whaaaaat import prompt

from modules.purger.purger import Purger
from modules.postprocessor.postprocessor import Postprocessor


def select_languages(available_langs: list[str], current_langs: list[str]) -> list[str]:
    current_langs = [language.upper() for language in current_langs]
    include_all = 'all' in current_langs
    options = [
        {'name': option, 'value': option, 'checked': include_all or option in current_langs}
        for option in available_langs
    ]
    answer = prompt({
        'name': 'langs',
        'message': 'Select the languages to purge',
        'type': 'checkbox',
        'choices': options
    })
    return answer['langs']


@click.group(help="Tool to purge your raw fbl datasets and to postprocess them")
def cli():
    pass


@cli.command(help='Purges raw fbl datasets uploading them on MongoDB')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help='Folder containing the raw datasets')
@click.option('-l', '--langs', type=click.STRING, multiple=True, default=[], show_default=True, help='Languages to purge. If "all" is passed, all the languages are selected')
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help='The name of the MongoDB database')
@click.option('-t', '--threshold', type=click.INT, default=int(1e6), show_default=True, help='Threshold of how many profiles will be buffered before being flushed on the database')
@click.option('-b', '--bias', type=click.INT, default=int(100e6), show_default=True, help='If octopus is set, there will be conflicts for the line field of assets of the same language. To overcome this conflict, this bias is added to the line field, multiplied by the index of the asset')
@click.option('-p', '--parallel/--no-parallel', is_flag=True, show_default=True, help='If uploading more than a language, purge these languages in parallel')
@click.option('--processes', type=click.INT, default=multiprocessing.cpu_count(), show_default=True, help='If parallel is active, specifies the number of parallel processes. Default is the number of cores of the CPU')
@click.option('-c', '--choose-langs/--no-choose-langs', is_flag=True, show_default=True, help='If the user will be asked to select the languages')
@click.option('-f', '--force/--no-force', is_flag=True, show_default=True, help='If already populated collections will be overriden. Overrides skip behaviour')
@click.option('--skip/--no-skip', is_flag=True, show_default=True, help='If when encountering an already populated collection it will be skipped')
@click.option('-o', '--octopus/--no-octopus', is_flag=True, show_default=True, help='If parallel is set, purge even the assets of a same language in parallel')
@click.option('-n', '--nazi/--no-nazi', is_flag=True, show_default=True, help='If it will fail as soon as an invalid line or error is encountered')
@click.option('--skip-first-line/--no-skip-first-line', is_flag=True, show_default=True, help='If a language has more han an asset, it could happen that a line is split between two assets. If this flag is enabled, the first line of all but the first assets is skipped.')
@click.option('-w', '--wide/--no-wide', is_flag=True, show_default=True, help='If also txt files and not only bz2 files will be considered')
def purge(*, src: str, langs: list[str], dbname: str, threshold: int, bias: int, parallel: bool, processes: int, choose_langs: bool, force: bool, skip: bool, octopus: bool, nazi: bool, skip_first_line: bool, wide: bool):
    purger = Purger(src)
    if choose_langs:
        available_langs = purger.available_langs
        langs = select_languages(available_langs, langs)
    purger.purge(langs, dbname, threshold, bias, parallel, processes, force, skip, octopus, nazi, skip_first_line, wide)


@cli.command(help='Postprocesses a raw collection into a parsed collection')
@click.option('-l', '--langs', type=click.STRING, multiple=True, default=[], show_default=True, help='Languages to process. If "all" is passed, all the languages are selected')
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help='The name of the MongoDB database')
@click.option('-t', '--threshold', type=click.INT, default=int(1e6), show_default=True, help='Threshold of how many profiles will be buffered before being flushed on the database')
@click.option('-p', '--parallel/--no-parallel', is_flag=True, show_default=True, help='If uploading more than a language, process these languages in parallel')
@click.option('--processes', type=click.INT, default=multiprocessing.cpu_count(), show_default=True, help='If parallel is active, specify the number of parallel processes. Default is the number of cores of the CPU.')
@click.option('-c', '--choose-langs/--no-choose-langs', is_flag=True, show_default=True, help='If the user will be asked to select the languages')
@click.option('-f', '--force/--no-force', is_flag=True, show_default=True, help='If already parsed collections will be overriden. Overrides skip behaviour')
@click.option('--skip/--no-skip', is_flag=True, show_default=True, help='If when encountering an already parsed collection it will be skipped')
@click.option('-n', '--nazi/--no-nazi', is_flag=True, show_default=True, help='If it will fail as soon as an invalid line or error is encountered')
def process(*, langs: list[str], dbname: str, threshold: int, parallel: bool, processes: int, choose_langs: bool, force: bool, skip: bool, nazi: bool):
    postprocessor = Postprocessor(dbname)
    if choose_langs:
        available_langs = postprocessor.available_langs
        langs = select_languages(available_langs, langs)
    postprocessor.process(langs, threshold, parallel, processes, force, skip, nazi)


@cli.group(help="Writes the available langs")
def langs():
    pass


@langs.command(help='Shows the languages available in the raw datasets')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help='Folder containing the raw datasets')
def raw(*, src: str):
    purger = Purger(src)
    langs = purger.available_langs
    langs_list = "\n".join(langs)
    click.echo(click.style('Available langs are:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))


@langs.command(help='Shows the languages available in MongoDB')
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help='Name of the MongoDB database')
def purged(*, dbname: str):
    postprocessor = Postprocessor(dbname)
    langs = postprocessor.available_langs
    langs_list = "\n".join(langs)
    click.echo(click.style('Available langs are:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))


if __name__ == '__main__':
    cli()
