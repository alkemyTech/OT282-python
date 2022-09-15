# Import the required libraries
import xml.etree.ElementTree as ET
import operator

readed_file ="./112010 Meta Stack Overflow/posts.xml"
tree = ET.parse(readed_file)
root= tree.getroot()
value=1
ar = {}
final=[]
#Import the XML file
def mapper(root):
    # Iterate over the data
    for line in root.findall('row'):
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
    return final


# Implement the reducer function and read the output of the previous function
def reducer(final):
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
    reducer(mapper(root))


