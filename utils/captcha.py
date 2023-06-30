from six import StringIO
from io import BytesIO

from PIL import Image
from pytesseract import image_to_string

import urllib

class CaptchaSolver:

    def __init__(self):
        self.config = '-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ -c oem=1 -c psm=13'
        self.offset = 0
        self.most_black = 40
        self.angles = [10, -15, 20, -15, 10, -10]

    def solve_captcha(self, url):
        initial_captcha_image = self.get_image_from_url(url)
        width, height = initial_captcha_image.size
        image = Image.new('L', (width, height), 'white')
        image.paste(initial_captcha_image.crop((10, 0, width, height)))
        image.paste(initial_captcha_image.crop((0, 0, 10, height)), (width-10, 0))

        for most_black in range(1, self.most_black+1)[::-1]:
            borders = self.get_borders(image, most_black)
            if len(borders) == 7:
                break

        divided_letters = self.devide_letters(image, borders)
        print('divided_letters>>', divided_letters)
        result_image = self.draw_image_from_letters(divided_letters)
        print('result_image>>', result_image)
        answer = image_to_string(
            result_image,
            config=self.config
        ).strip().replace(' ', '')

        initial_captcha_image.close()
        image.close()
        result_image.close()

        return answer

    def get_borders(self, image, most_black):
        _borders = []
        borders = [self.offset]

        width, height = image.size
        for w in range(width):
            black_pixels = 0
            for h in range(height):
                pixel = image.getpixel((w, h))
                if pixel < most_black:
                    black_pixels += 1
            if black_pixels >= 2:
                _borders.append(w)

        _borders = sorted(list(set(range(_borders[0], _borders[-1]+1)) - set(_borders)))
        for num in _borders:
            if num > borders[-1]+15:
                borders.append(num+1)
        borders.append(width)
        return borders

    def devide_letters(self, image, borders):
        letters = []
        for n in range(0, len(borders) - 1):
            _image = Image.new('RGBA', (borders[n + 1] - borders[n], 50))
            for i, v in enumerate(range(borders[n], borders[n + 1])):
                for h in range(50):
                    _image.putpixel((i, h), (image.getpixel((v, h)),) * 3)
            try:
                letters.append(_image.rotate(self.angles[n], expand=True, resample=Image.BILINEAR))
            except:
                pass
        return letters

    @staticmethod
    def draw_image_from_letters(letters):
        result = Image.new('RGBA', (500, 150), 'white')
        positions = [10]
        for letter in letters:
            width, _ = letter.size
            result.paste(letter, (positions[-1], 0), mask=letter)
            positions.append(positions[-1] + width + 5)
        return result

    @staticmethod
    def get_image_from_url(url):
        request = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        contents = urllib.request.urlopen(request).read()
        img = Image.open(BytesIO(contents))
        img.save('cap.png', 'png')
        return img

# class AmazonSolver(object):

#     def __init__(self, spider, captcha_middleware):
#         self.spider = spider
#         self.captcha_middleware = captcha_middleware

#     def input_captcha(self, response, spider):
#         captcha_url = spider.get_captcha_key(response)
#         solution = CaptchaSolver().solve_captcha(captcha_url)
#         return self.spider.get_captcha_form(
#             response,
#             solution=solution,
#             referer=response.meta['initial_url'],
#             callback=self.captcha_middleware.captcha_handled,
#         ).replace(
#             dont_filter=True
#         )
