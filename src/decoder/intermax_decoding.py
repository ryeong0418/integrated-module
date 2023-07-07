
"""
Module Name: decoding
Module description goes here.
"""

import datetime
import urllib.parse


class DecodingJs:

    """ XAPM_BIND_SQL_ELAPSE 테이블에서 BIND_LIST값 디코딩하여 BIND_VALUE 값 추출하는 모듈 """

    def __init__(self):

        self.total_length = 0
        self.pos = 0
        self.byte = 2
        self.short = 4
        self.integer = 8
        self.double = 16
        self.result = []

    def convert_bind(self, bindlist_value):

        """ XAPM_BIND_SQL_ELAPSE 테이블에 있는 BIND_LIST값 디코딩하여 결과값 반환하는 Main 함수 """

        num_i = 0
        bind_name = 0

        total_length = self.h2d(self.bind_substring(bindlist_value, 2))
        bind_list_code_type = 'INDEX' if self.h2d(bindlist_value[2:4]) else 'NAME'

        while num_i < total_length:
            idx = self.h2d(self.bind_substring(bindlist_value,2))
            num_i += 1

            if bind_list_code_type == 'INDEX':
                code_type = self.h2d(self.bind_substring(bindlist_value,2))

            else:
                bind_name_length = self.h2d(self.bind_substring(bindlist_value,2))
                bind_name = self.h2c(self.bind_substring(bindlist_value,bind_name_length * 2))
                code_type = self.h2d(self.bind_substring(bindlist_value,2))

            bind_code = idx if bind_list_code_type == 'INDEX' else bind_name

            if code_type == 0:
                bind_value = None
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 1:
                bind_value = bool(self.h2d(self.bind_substring(bindlist_value,self.byte)))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 2:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.byte))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 3:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.short))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 4:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.integer))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 5:
                bind_value = self.h2l_by_bigint(self.bind_substring(bindlist_value,self.double))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 6:
                bind_value = self.h2f(self.bind_substring(bindlist_value,self.integer))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 7:
                bind_value = self.h2lf(self.bind_substring(bindlist_value,self.double))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 8:
                bind_value = self.h2l_by_bigint(self.bind_substring(bindlist_value,self.double))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 9:
                bing_length = self.h2d(self.bind_substring(bindlist_value,self.byte))
                bind_value = self.h2c(self.bind_substring(bindlist_value,bing_length * 2))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 10:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.double))
                bind_value = str(datetime.date.fromtimestamp(bind_value/1000))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 11:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.double))
                date_time = datetime.datetime.fromtimestamp(bind_value / 1000)
                bind_value = str(date_time.strftime("%H:%M:%S"))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 12:
                bind_value = self.h2d(self.bind_substring(bindlist_value,self.double))
                date_time = datetime.datetime.fromtimestamp(bind_value / 1000)
                bind_value = str(date_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 13:
                bing_length = self.h2d(self.bind_substring(bindlist_value,self.byte))
                bind_value = self.h2d(self.bind_substring(bindlist_value,bing_length * 2))
                self.result.append({'code': bind_code, 'value': bind_value})

            elif code_type == 14:
                bing_length = self.h2d(self.bind_substring(bindlist_value,self.short))
                bind_value = self.h2c(self.bind_substring(bindlist_value,bing_length * 2))
                self.result.append({'code': bind_code, 'value':  bind_value})

            else:
                pass

        return self.result

    def bind_substring(self,bindlist_value, num):

        """ BIND_LIST값을 주어진 범위만큼 일부 추출하는 함수 """

        scope = bindlist_value[self.pos:self.pos + num]
        self.pos = self.pos + num
        return scope

    def h2d(self, val):

        """ 16진수로 표현된 문자열 val을 10진수 정수로 변환 """

        try:
            hex_num = int(val, 16)  # val을 16진수로 변경
            return hex_num

        except ValueError:
            hex_num = None
            return hex_num

    def h2f(self,val):

        """ BIND_LIST값 일부를 실수형으로 변환하는 함수 (hex to float) """

        hex_val = int('0x' + val, 16)  # '0x'+val을 16진수로 변경
        part1 = (hex_val & 0x7fffff | 0x800000) * 1.0
        part2 = pow(2, 23)
        part3 = pow(2, ((hex_val >> 23 & 0xff) - 127))

        return (part1 / part2) * part3

    def h2l(self, val):

        """ 브라우저에서 BigInt를 지원하지 않는 경우 호출
            모듈에서 사용하지 않는 함수이지만 Javascript 코드에 있어서 첨부함 """

        dec = '0'

        def add(num_x, num_y):
            con = 0
            r_list = []
            num_x = list(map(int, num_x))
            num_y = list(map(int, num_y))

            while num_x or num_y:
                num_s = (num_x.pop() if num_x else 0) + (num_y.pop() if num_y else 0) + con
                r_list.insert(0, num_s if num_s < 10 else num_s - 10)
                con = 0 if num_s < 10 else 1

            if con:
                r_list.insert(0, con)

            return ''.join(map(str, r_list))

        for i in list(val):

            decimal_number = int(i, 16)
            bit_val = 8

            while bit_val:

                dec = add(dec, dec)

                if decimal_number & bit_val:
                    dec = add(dec, '1')

                bit_val >>= 1

        return dec

    def h2l_by_bigint(self, val):

        """
        BIND_LIST값 일부를 BigInt형식의 값(int_num)으로 반환하는 로직
        브라우저에서 BigInt를 지원하는 경우 호출
        """

        if not val or val == '':
            print('Not found value in bindlist_value')
            return ''

        if len(val) % 2:
            val = '0' + val

        high_byte = int(val[0:2], 16)
        int_num = int('0x' + val, 16)

        if 0x80 & high_byte:
            int_num = int(''.join(['1' if i == '0' else '0' for i in bin(int_num)[2:]]), 2) + 1
            int_num = -int_num

        return int_num

    def h2lf(self,val):

        """ BIND_LIST값 일부를 실수형으로 변환하는 함수 (hex to double) """

        high = int('0x' + val[:8], 16)
        low = int('0x' + val[8:16], 16)
        e_num = ((high >> (52 - 32)) & 0x7ff) - 1023

        part1 = ((high & 0xfffff) | 0x100000) * 1.0

        return part1 / pow(2, 52 - 32) * pow(2, e_num) + low * 1.0 / pow(2, 52) * pow(2, e_num)

    def h2c(self, val):

        """ BIND_LIST값 일부를 character로 변환하는 함수 """

        num_i = 0
        b_str = ''
        val_len = len(val)
        result = ''
        result_list = []

        try:

            for i in range(num_i, val_len, 2):
                result_list.append(int(val[i:i + 2], 16))
                b_str = urllib.parse.quote(bytes(result_list))

            return urllib.parse.unquote(b_str)

        except :
            return result

        finally:
            num_i = None
            b_str = None
