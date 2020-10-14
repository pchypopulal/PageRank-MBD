PageRank algorithm implementation which makes use of the Hadoop framework.

# Execute the program
- Use modified dataset web-Google.txt. I deleted comment words and left pure data.
- Running InputGenerator.java first to generate the input file (part-r-00000) for the next process. Set args[0] as "web-Google.txt" and args[1] as the output directory.
- Running PageRank.java to get the PageRank value for every node. Please note that it is important to make sure the input path points to the file "part-r-00000". This program will iterate 75 times. （Actually 20 times iteration is enough to get a relatively accurate result.)
- Use filter.ipynb (https://drive.google.com/open?id=1m6uBxaCKt637Lu8wahYYqi3oFK2vmpaj) to generate the ordered list of the ten nodes having the largest PageRank.

# Illustration
According to comments in the file web-Google.txt, this dataset has 875713. I used the breadth-first search algorithm to traverse all nodes and found that only 600493 nodes are connected. As the BFS.java was found on the internet, I didn't include it in my code.