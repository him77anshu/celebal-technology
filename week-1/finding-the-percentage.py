if __name__ == '__main__':
    n = int(input())
    student_marks = {}
    for _ in range(n):
        line = input().split()
        name = line[0]
        scores = list(map(float, line[1:]))
        student_marks[name] = scores
    query_name = input()
    average = sum(student_marks[query_name]) / len(student_marks[query_name])
    print("{:.2f}".format(average))