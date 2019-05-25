

path = 'logs/directories_uploaded_Date_2019-05-22_Time_H11-M48.log'
def words_in_text(path):
    with open(path) as handle:
        for line in handle:
            words = line.split()
            if words[0] == 'Completed':
                yield words[-1:]





