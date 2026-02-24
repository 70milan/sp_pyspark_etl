#linear algo

#O(n)


def linear_search(list, target):
    '''
    returns the index position else returns none
    '''
    for i in range(0, len(list)):
        if list[i] == target:
            return i
    return None


def verify(index):
    if index is not None:
        print("target found at index: ", index)
    else:
        print("targent not found in list")

numbers = [1,2,3,4,5,6,7,8,9,10]



result = linear_search(numbers, 3)
verify(result)


#Binary Search algo

#O(log n)
#Version 1
def binary_search(list, target):
    first = 0
    last = len(list) - 1
    while first <= last:
        midpoint = (first+last)//2
        if list[midpoint] == target:
            return midpoint
        elif list[midpoint] < target:
            first = midpoint + 1
        else:
            last = midpoint -1
    return None

def verify(index):
    if index is not None:
        print("result is at ", index)
    else: 
        print("result not found")


numbers = [1,2,4,3,5,6,7,8,9,10]

result = binary_search(numbers, 7)
verify(result)





#Recursive Binary Search



def recursive_binary_search(list, target):
    if len(list) == 0:
        return False
    else:
        midpoint = len(list)//2
        if list[midpoint] == target:
            return True
        else:
            if list[midpoint] < target:
                return recursive_binary_search(list[midpoint+1:], target)
            else:
                return recursive_binary_search(list[:midpoint], target)
            

def verify_rec(result):
    print("Target is ", result)

numbers = [1,2,4,3,5,6,7,8,9,10]
result = recursive_binary_search(numbers, 322)
verify_rec(result)

result1 = recursive_binary_search(numbers, 1)
verify_rec(result1)
