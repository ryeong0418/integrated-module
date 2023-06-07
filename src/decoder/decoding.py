import datetime
from urllib.parse import quote_plus
from urllib import parse
import urllib.parse

class Decoding:

    def convertBindList(bindList):

        bindlist = {'bind_list': 0}
        bindlist['bind_list'] = bindList
        ix = 0
        totalLength = 0
        global pos
        pos = 0

        byte = 2
        short = 4
        integer = 8
        double = 16
        result = []

        def bindSubString(range):
            global pos
            str = bindList[pos:pos + range]
            pos += range
            return str

        def h2d(val):

            try:
                hex_num = int(val, 16)  # val을 16진수로 변경
                return hex_num

            except ValueError:
                hex_num = None
                return hex_num

        def h2f(val):
            hex = int('0x' + val, 16)  # '0x'+val을 16진수로 변경
            return (hex & 0x7fffff | 0x800000) * 1.0 / pow(2, 23) * pow(2, ((hex >> 23 & 0xff) - 127))
        def h2l(val):

            dec = '0'

            def add(x, y):
                c = 0
                r = []
                x = list(map(int, x))
                y = list(map(int, y))

                while len(x) or len(y):
                    s = (x.pop() if len(x) else 0) + (y.pop() if len(y) else 0) + c
                    r.insert(0, s if s < 10 else s - 10)
                    c = 0 if s < 10 else 1

                if c:
                    r.insert(0, c)

                return ''.join(map(str, r))

            for chr in list(val):
                n = int(chr, 16)
                t = 8
                while t:
                    dec = add(dec, dec)
                    if (n & t):
                        dec = add(dec, '1')
                    t >>= 1
            return dec

        def h2lByBigInt(val):
            if not val or not len(val) or val == '':
                print('Not found value in BindList')
                return ''

            if len(val) % 2:
                val = '0' + val

            highbyte = int(val[0:2], 16)
            bn = int('0x' + val, 16)

            if 0x80 & highbyte:
                bn = int(''.join(['1' if i == '0' else '0' for i in bin(bn)[2:]]), 2) + 1
                bn = -bn

            return bn

        def h2lf(val):
            high = int('0x' + val[:8], 16)
            low = int('0x' + val[8:16], 16)
            e = ((high >> (52 - 32)) & 0x7ff) - 1023

            return ((high & 0xfffff) | 0x100000) * 1.0 / pow(2, 52 - 32) * pow(2, e) + low * 1.0 / pow(2, 52) * pow(2, e)

        def h2c(val):
            b_str = ''
            ix = 0
            ixLen = len(val)
            result = ''
            result_list = []

            try:

                for i in range(0,ixLen,2):
                    result_list.append(int(val[i:i + 2], 16))
                    b_str = parse.quote(bytes(result_list))

                return urllib.parse.unquote(b_str)

            except:
                return ''

            finally:
                ix = None
                b_str = None

        if bindList:

            totalLength = h2d(bindSubString(2))
            bindListType = 'INDEX' if h2d(bindList[2:4]) else 'NAME'

            ix = 0
            while ix < totalLength:
                idx = h2d(bindSubString(2))
                ix += 1

                if bindListType == 'INDEX':
                    type = h2d(bindSubString(2))

                else:
                    bindNameLength = h2d(bindSubString(2))
                    bindName = h2c(bindSubString(bindNameLength * 2))
                    type = h2d(bindSubString(2))

                if bindListType == 'INDEX':
                    bindCode = idx

                else:
                    bindCode = bindName

                if type == 0:
                    bindValue = None
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 1:
                    bindValue = bool(h2d(bindSubString(byte)))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 2:
                    bindValue = h2d(bindSubString(byte))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 3:
                    bindValue = h2d(bindSubString(short))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 4:
                    bindValue = h2d(bindSubString(integer))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 5:
                    bindValue=h2lByBigInt(bindSubString(double))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 6:
                    bindValue = h2f(bindSubString(integer))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 7:
                    bindValue = h2lf(bindSubString(double))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 8:
                    bindValue = h2lByBigInt(bindSubString(double))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 9:
                    bindLength = h2d(bindSubString(byte))
                    bindValue = h2c(bindSubString(bindLength * 2))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 10:
                    bindValue = h2d(bindSubString(double))
                    bindValue = str(datetime.date.fromtimestamp(bindValue/1000))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 11:
                    bindValue = h2d(bindSubString(double))
                    bindValue = str(datetime.datetime.fromtimestamp(bindValue/1000).strftime("%H:%M:%S"))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 12:
                    bindValue = h2d(bindSubString(double))
                    bindValue = str(datetime.datetime.fromtimestamp(bindValue/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 13:
                    bindLength = h2d(bindSubString(byte))
                    bindValue = h2d(bindSubString(bindLength * 2))
                    result.append({'code': bindCode, 'value': bindValue})

                if type == 14:
                    bindLength = h2d(bindSubString(short))
                    bindValue = h2c(bindSubString(bindLength * 2))
                    result.append({'code': bindCode, 'value':  bindValue})

                else:
                    pass

        return result




