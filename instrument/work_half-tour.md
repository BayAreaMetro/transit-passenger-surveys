# At-Work Tour Logic


The below table explains cases in which we ask information about the half-tour for work We want to know if they were at work prior to their trip or intend 
to be at work after their trip, according to the below coming from/going to logic.


## Cases

| **Case** | **Trip Origin**        | **Trip Destination**   | **Employed?**                  | **Ask if at work before trip** | **Ask if at work after trip**  |
|:---------|:-----------------------|:-----------------------|:-------------------------------|:-------------------------------|:-------------------------------|
| 1.       | Home                   | Work                   | Don't ask                      | Don't ask                      | Don't Ask                      |
| 2.       | Home                   | Other                  | Ask                            | Don't ask                      | Ask (if employed = yes)        |
| 3.       | Work                   | Home                   | Don't ask                      | Don't ask                      | Don't Ask                      |
| 4.       | Work                   | Other                  | Don't ask                      | Don't ask                      | Ask                            |
| 5.       | Other                  | Home                   | Ask                            | Ask (if employed = yes)        | Don't ask                      |
| 6.       | Other                  | Work                   | Don't ask                      | Ask                            | Don't ask                      |
| 7.       | Other                  | Other                  | Ask (if employed = yes)        | Ask (if employed = yes)        | Ask (if employed = yes)        |

 <br/>
     
## "Other" origin or destination; home-work-other cases

```
1.  Work-related
2.  Hotel (visitors only)
3.  Social or recreational
4.  Shopping
5.  College/university (student only)
6.  School (K-12) (student only)
7.  Airport (airline passengers only)
8.  Medical/dental
9.  Dining/coffee
10. Escorting others (pick up/drop off)
11. Personal business
12. Other: (any text)    
```
<br/>
[Return to summary](README.md/#half-tour-questions-for-work-and-school)


