#!/usr/bin/env python
import click
import multiprocessing
import modules.purger.purger as purger

@click.group(help="Tool to purge, raw and postprocess your raw fbl datasets")
def cli():
    pass

@click.command(help='Purges raw fbl datasets uploading them on MongoDB')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help="Folder containing the raw datasets")
@click.option('-l', '--langs', type=click.STRING, multiple=True, default=['all'], show_default=True, help="Languages to purge")
@click.option('-d', '--dbname', type=click.STRING, default='fbl', show_default=True, help="Database name")
@click.option('-t', '--threshold', type=click.INT, default=int(1e6), show_default=True, help="Threshold of profiles that can be parsed without being uploaded to the db")
@click.option('-p', '--parallel/--no-parallel', is_flag=True, show_default=True, help="If uploading more than a language, parallelize the uploadings")
@click.option('-t', '--threads', type=click.INT, default=multiprocessing.cpu_count(), show_default=True, help="If parallel is active, specify the number of parallel processes. Default is the number of cores of the CPU.")
def purge(*, src: str, langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int):
    purger.purge(src, langs, dbname, threshold, parallel, threads)

@click.command(help='Shows the languages available in the dataset')
@click.option('-s', '--src', type=click.STRING, default='datasets', show_default=True, help="Folder containing the raw datasets")
def langs(*, src: str):
    purger.langs(src)


cli.add_command(purge)
cli.add_command(langs)

if __name__ == '__main__':
    cli()