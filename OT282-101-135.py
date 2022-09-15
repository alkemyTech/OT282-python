# Import the required libraries
import xml.etree.ElementTree as ET
import operator


#Import the XML file
def mapper(file):
    tree = ET.parse(file)
    root= tree.getroot()
    readed_file = root
    return readed_file

# Implement the reducer function and read the output of the previous function
def reducer(data):
    final=[]
    ar = {}
    value=1
    # Iterate over the data
    for line in data.findall('row'):
    # If the attribute is in the row
        if 'AcceptedAnswerId' in line.attrib:
            # Take off the TAG attribute and separate each word
            atribs = line.attrib['Tags'].rsplit(sep= '<')
            # Final clean of the word and append to the dictionary
            for i in atribs:
                final.append(i.replace('>','').strip())
    
    # Final clean                    
    for i in final:
        if i=='':
            final.remove('')

    # Asign the value to each word and add the value if this already exists
    for item in final:
        #print(item)
        if item not in ar:
            ar[item]= value
        else:
            ar[item]+= value

    # Printing the original dictionare
    print(ar)
    # Sort the dyctionare and prnt it
    ar_sort = sorted(ar.items(), key=operator.itemgetter(1), reverse=True)[:10]
    print('\n')
    for j in ar_sort:
        print(j)

# Run the functions
if __name__=="__main__":
    readed_file = mapper("./112010 Meta Stack Overflow/posts.xml")
    reducer(readed_file)


