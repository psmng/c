cd ./

for cnt in {1..100}
do
    echo "[$cnt] dscd 테스트 시작"
    ./dscd https://discord.com/api/webhooks/1535579952819212378/JrFMu1saNj3q2NqidIGTd10DXYJgFnzT_hFwpwMrBws7OtNYduBkmiQ36KDqSPfIVRwg "TEST=$cnt"
done

