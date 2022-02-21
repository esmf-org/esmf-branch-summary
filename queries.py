LAST_HASH = lambda x: "SELECT hash as 'last hash' FROM summaries WHERE branch = ? order by modified desc limit 1;", x

