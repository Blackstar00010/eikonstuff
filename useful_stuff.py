import winsound
import platform
import os


def beep(duration=1):
    """
    Makes a beep sound
    :param duration: the duration of the 440Hz sound in seconds
    :return: None
    """
    freq = 440
    if platform.system() == 'Darwin':
        os.system(f'play -nq -t alsa synth {duration} sine {freq}')
    else:
        winsound.Beep(freq, duration*1000)


if __name__ == '__main__':
    beep()
