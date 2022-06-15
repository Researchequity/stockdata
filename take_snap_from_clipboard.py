from PIL import ImageGrab
import sys


name = sys.argv[1]


def convert_img_to_png(name):
    img = ImageGrab.grabclipboard()
    img.save(name, 'PNG')


convert_img_to_png(name)

# call this from commanline or excel for image
# D:\Python\python.exe D:/Python/rohan/take_snap_from_clipboard.py D:\Program\python_ankit\ProjectDir\metadata\paste.jpg
#D:\Program\bse_shareholding_pattern_scrapping\env\Scripts\python.exe D:/Program/python_ankit/ProjectDir/master/take_snap_from_clipboard.py D:\Program\python_ankit\ProjectDir\metadata\paste.jpg
