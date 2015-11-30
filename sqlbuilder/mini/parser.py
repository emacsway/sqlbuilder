import sqlparse


class Parser(object):

    def __init__(self, sql, list_words=()):
        self._sql = sql
        self._list_words = list_words

    def _handle_level(self, stmt):
        result = []
        for token in stmt.tokens:
            if token.is_group():
                result.append(self._handle_level(token))
            else:
                if token.is_whitespace():
                    continue
                if token.match(sqlparse.tokens.Punctuation, ','):
                    if isinstance(token.parent, sqlparse.sql.IdentifierList):
                        continue
                result.append(str(token))
        return result

    def to_hierarchical_list(self):
        parsed = sqlparse.parse(self._sql)
        stmt = parsed[0]
        return self._handle_level(stmt)


if __name__ == '__main__':
    sql = """select f1, f2 from "someschema"."mytable" where id = 1 and f2 = 3 group by f2, f3 order by id, f2 DESC"""
    import pprint
    pprint.pprint(Parser(sql).to_hierarchical_list())
    # print sqlparse.format(sql, reindent=True, keyword_case='upper')
