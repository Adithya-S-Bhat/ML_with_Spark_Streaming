import matplotlib.pyplot as plt
import pickle

#getting data using pickle

with open('spam.pkl','rb') as f1:
    spam_count_viz=pickle.load(f1)
    ham_count_viz=pickle.load(f1)

total_spam=sum(spam_count_viz)
total_ham=sum(ham_count_viz)
total_count=total_spam+total_ham
#pie chart 
labels=['Spam','Ham']
sizes=[total_spam/total_count,total_ham/total_count]
explode=[0,0.1]
colors=['#99ff99','#ff9999']
fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=90)
ax1.axis('equal')
plt.tight_layout()
plt.savefig("Spam_and_Ham_pie_chart")
plt.close(fig1)


#line charts multiple 

batches=list(range(len(spam_count_viz)))
""" print(len(spam_count_viz))
print(batches) """
spam_chart1 = plt.plot(batches, spam_count_viz, color='Red')
ham_chart2 = plt.plot(batches, ham_count_viz, color='Blue')
plt.xlabel('Batch',color="green")
plt.ylabel('Count',color="green")
plt.title('Spam/Ham per batch')
plt.legend(['Spam', 'Ham'], loc=3)
plt.savefig("Spam_Ham_per_batch")
plt.close()




