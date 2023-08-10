import useful_stuff as us
import os


def check_size(directory, size_limit=100):
    """
    Checks sizes of files
    :param directory:
    :param size_limit:
    :return:
    """
    files = us.listdir(directory, file_type=None)
    for afile in files:
        if os.path.isfile(directory+afile):
            file_size = os.stat(directory+afile).st_size / 1024 / 1024
            if file_size > size_limit:
                print(f'{directory+afile} has file size of {round(file_size, 2)}MB.')
        elif os.path.isdir(directory+afile):
            check_size(directory+afile+'/', size_limit=size_limit)


if __name__ == '__main__':
    with open('../.gitignore') as f:
        gitignore = f.read().split('\n')
    gitignore.remove('')
    for athing in gitignore:
        if athing[0] == '#':
            gitignore.remove(athing)
    print(gitignore)
    check_size('../')
