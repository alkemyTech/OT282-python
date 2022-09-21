import pytest
import xml.etree.ElementTree as ET
from avg_score_most_fav_ans import reducer, chunkify, gets_score_fav, mapper


@pytest.fixture
def tree_node():
    xml = """<?xml version="1.0" encoding="utf-8"?>
<row Id="6" PostTypeId="1" CreationDate="2009-06-28T08:40:18.673" Score="5" ViewCount="249" Body="&lt;p&gt;When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?&lt;/p&gt;&#xA;" OwnerUserId="41673" LastEditorUserId="23354" LastEditorDisplayName="" LastEditDate="2009-07-10T13:49:41.953" LastActivityDate="2010-04-14T06:58:25.480" Title="Should StackOverflow support more than 2 openids per account?" Tags="&lt;feature-request&gt;&lt;status-declined&gt;&lt;openid&gt;&lt;login&gt;&lt;google&gt;" AnswerCount="4" CommentCount="3" FavoriteCount="0" />
  """
    return ET.fromstring(xml)


@pytest.fixture
def tree_node_chunks():
    xml = """<?xml version="1.0" encoding="utf-8"?>
    <posts>
<row Id="6" PostTypeId="1" CreationDate="2009-06-28T08:40:18.673" Score="5" ViewCount="249" Body="&lt;p&gt;When using Google for your OpenId provider, it generates a different openid url for each website you use with it.  This means that for stackoverflow.com, meta.stackoverflow.com, superuser.com and serverfault.com you will have 4 different openids.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Currently you can have an main and an alternate openid - should the system support as moany openids as there are sites in the stackoverflow family?&lt;/p&gt;&#xA;" OwnerUserId="41673" LastEditorUserId="23354" LastEditorDisplayName="" LastEditDate="2009-07-10T13:49:41.953" LastActivityDate="2010-04-14T06:58:25.480" Title="Should StackOverflow support more than 2 openids per account?" Tags="&lt;feature-request&gt;&lt;status-declined&gt;&lt;openid&gt;&lt;login&gt;&lt;google&gt;" AnswerCount="4" CommentCount="3" FavoriteCount="0" />
<row Id="11" PostTypeId="1" CreationDate="2009-06-28T09:19:00.827" Score="3" ViewCount="85" Body="&lt;p&gt;Take a look at &lt;a href=&quot;http://stackoverflow.com/users/111786/bbuser&quot;&gt;&lt;code&gt;bbuser&lt;/code&gt;&lt;/a&gt;.&#xA;The user has not entered a name in his profile.&#xA;I don't know where the name &lt;code&gt;bbuser&lt;/code&gt; comes from, but I assume it is somehow part of his OpenID.&#xA;He appears as &lt;code&gt;bbuser&lt;/code&gt; in questions, answers and elsewhere on &lt;code&gt;stackoverflow.com&lt;/code&gt; though.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;strong&gt;Is is not possible to find the user by searching for &lt;code&gt;bbuser&lt;/code&gt;&lt;/strong&gt;.&lt;br/&gt;&#xA;It would be cool if this would work and here is why:&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I told two people about &lt;code&gt;stackoverflow.com&lt;/code&gt; recently and both were very interested.&#xA;After a while I asked them if they had subscribed and they told me their ID.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I could not find any them with user search.&#xA;It happened to be that both of them had not bothered to enter a username in their profile.&#xA;I guess that this is quite common for new users.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;strong&gt;In my opinion it would be useful if we could search for users by the parts of their OpenID that is used to identify them at &lt;code&gt;stackoverflow.com&lt;/code&gt;.&lt;/strong&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Interestingly a google search for &quot;&lt;code&gt;bbuser stackoverflow&lt;/code&gt;&quot; returns the users profile as result.&lt;/p&gt;&#xA;" OwnerUserId="84671" LastEditorUserId="106" LastEditorDisplayName="" LastEditDate="2009-07-10T14:49:47.770" LastActivityDate="2009-07-10T14:49:47.770" Title="Search for users per (part of) OpenID" Tags="&lt;feature-request&gt;&lt;search&gt;&lt;openid&gt;" AnswerCount="2" CommentCount="2" /> 
<row Id="16" PostTypeId="1" AcceptedAnswerId="95" CreationDate="2009-06-28T09:31:24.150" Score="2" ViewCount="168" Body="&lt;p&gt;While playing with the data dump for a blog post (&lt;a href=&quot;http://lanai.dietpizza.ch/geekomatic/2009/06/11/1244725080000.html&quot; rel=&quot;nofollow&quot;&gt;Stack Overflow: Badge Analysis Over Time&lt;/a&gt;), I see that no Popular Question badges awarded for 27 May, which is odd.    &lt;/p&gt;&#xA;&#xA;&lt;p&gt;The missing badges show in both the absolute graph (first) and the relative graph (second), as we see below.  The relative graph is great for seeing the relative trends, and also to see if the badges were or were not retroactively...it looks to me as if they were not awarded.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;img src=&quot;http://lanai.dietpizza.ch/images/stack-overflow-missing-badges-mid-may.png&quot; alt=&quot;Absolute graph&quot; /&gt;&#xA;&lt;img src=&quot;http://lanai.dietpizza.ch/images/stack-overflow-80-20-badges-mid-may.png&quot; alt=&quot;Relative graph&quot; /&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Did this really happen?  Or is there a fault somewhere?  At no point since the early days has there ever been no Popular Question badges awarded, especially mid-week.&lt;/p&gt;&#xA;" OwnerUserId="2961" LastEditorUserId="106" LastEditorDisplayName="" LastEditDate="2009-07-10T14:51:07.237" LastActivityDate="2009-07-10T14:51:07.237" Title="No Popular Question badges awarded for 27 May?" Tags="&lt;discussion&gt;&lt;badges&gt;&lt;data-dump&gt;" AnswerCount="3" CommentCount="2" />
</posts>
"""
    rows = ET.fromstring(xml)
    data_chunks = chunkify(rows, 100)
    return data_chunks


def test_reducer():
    """test reducer function with two dict"""
    dict1 = {1: [0, 1], 2: [1, 2, 3]}
    dict2 = {2: [0, 1], 3: [1, 2, 3]}

    r = reducer(dict1, dict2)

    assert r == {1: [0, 1], 2: [1, 2, 3, 0, 1], 3: [1, 2, 3]}


def test_reducer_empty_dict():
    """test reducer function with an empty dict"""
    d1 = {2: 1, 3: 1, 4: 1}
    d2 = {}
    r = reducer(d1, d2)

    assert r == {2: 1, 3: 1, 4: 1}


def test_gets_score_fav(tree_node):
    """test gets_score_fav function"""
    r = gets_score_fav(tree_node)

    assert r == [0, 5]


def test_mapper(tree_node_chunks):
    """Test mapper function"""
    r = list(map(mapper, tree_node_chunks))
    assert r == [{0: [5]}]
