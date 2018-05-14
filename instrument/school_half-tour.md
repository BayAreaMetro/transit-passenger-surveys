# At-School Tour Logic


The below table explains cases in which we ask information about the half-tour for school We want to know if they were at school prior to their trip or intend 
to be at school after their trip, according to the below coming from/going to logic.


## Cases

| **Case** | **Trip origin**        | **Trip destination**   | **Student?**                   | **Ask if at school before trip** | **Ask if at school after trip**|
|:---------|:-----------------------|:-----------------------|:-------------------------------|:---------------------------------|:-------------------------------|
| 1.       | Home                   | School                 | Don't ask                      | Don't ask                        | Don't Ask                      |
| 2.       | Home                   | Other                  | Ask                            | Don't ask                        | Ask (if student = yes)         |
| 3.       | School                 | Home                   | Don't ask                      | Don't ask                        | Don't Ask                      |
| 4.       | School                 | Other                  | Don't ask                      | Don't ask                        | Ask                            |
| 5.       | Other                  | Home                   | Ask                            | Ask (if student = yes)           | Don't ask                      |
| 6.       | Other                  | School                 | Don't ask                      | Ask                              | Don't ask                      |
| 7.       | Other                  | Other                  | Ask                            | Ask (if student = yes)           | Ask (if student = yes)         |

 <br/>
     
## "Other" origin or destination; home-school-other cases

```
1.  Usual workplace
2.  Work-related
3.  Hotel (visitors only)
4.  Social or recreational
5.  Shopping
6.  Airport (airline passengers only)
7.  Medical/dental
8.  Dining/coffee
9.  Escorting others (pick up/drop off)
10. Personal business
11. Other: (any text)    
```
<br/>  

[Return to summary](README.md/#half-tour-questions-for-work-and-school)
