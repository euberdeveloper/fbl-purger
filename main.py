#!/usr/bin/env python
import click
import multiprocessing
from whaaaaat import prompt

import modules.purger.purger as purger
import modules.postprocessor.postprocessor as postprocessor

def select_languages(available_langs: list[str], current_langs: list[str]) -> list[str]:
    current_langs = [l.upper() for l in current_langs]
    include_all = 'all' in current_langs
    options = [
        { 'name': option, 'value': option, 'checked': include_all or option in current_langs }
        for option in available_langs
    ]
    answer = prompt({
        'name': 'langs',
        'message': 'Select the languages to purge',
        'type': 'checkbox',
        'choices': options
    })
    return answer['langs']

@click.group(help="Tool to purge your raw fbl datasets and to postprocess your purged ones")
def cli():
    pass

@cli.command(help='Purges raw fbl datasets uploading them on MongoDB')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help='Folder containing the raw datasets')
@click.option('-l', '--langs', type=click.STRING, multiple=True, default=[], show_default=True, help='Languages to purge. If "all" is passed, all the languages are selected.')
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help='Database name')
@click.option('-t', '--threshold', type=click.INT, default=int(1e6), show_default=True, help='Threshold of profiles that can be parsed without being uploaded to the db')
@click.option('-p', '--parallel/--no-parallel', is_flag=True, show_default=True, help='If uploading more than a language, parallelize the uploadings')
@click.option('--threads', type=click.INT, default=multiprocessing.cpu_count(), show_default=True, help='If parallel is active, specify the number of parallel processes. Default is the number of cores of the CPU.')
@click.option('-c', '--choose-langs/--no-choose-langs', is_flag=True, show_default=True, help='If the user will be asked to select the languages')
@click.option('-f', '--force/--no-force', is_flag=True, show_default=True, help='If already populated collections will be overriden. Overrides skip behaviour.')
@click.option('--skip/--no-skip', is_flag=True, show_default=True, help='If uploading more than a language, parallelize the uploadings')
def purge(*, src: str, langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int, choose_langs: bool, force: bool, skip: bool):
    if choose_langs:
        available_langs = purger.langs(src)
        langs = select_languages(available_langs, langs)
    purger.purge(src, langs, dbname, threshold, parallel, threads, force, skip)


@cli.group(help="Writes the available langs")
def langs():
    pass

@langs.command(help='Shows the languages available in the raw datasets')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help='Folder containing the raw datasets')
def raw(*, src: str):
    langs = purger.langs(src)
    langs_list = "\n".join(langs)
    click.echo(click.style('Available langs are:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))

@langs.command(help='Shows the languages available in the purged database')
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help='Name of the MongoDB database')
def raw(*, dbname: str):
    langs = postprocessor.langs(dbname)
    langs_list = "\n".join(langs)
    click.echo(click.style('Available langs are:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))

if __name__ == '__main__':
    cli()