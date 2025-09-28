package utils

import "testing"
import "github.com/stretchr/testify/require"

var testData = []byte("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z")
var testData1 = []byte("aa")
var testData2 = []byte("aa,bb")
var testData3 = []byte("aa,bb,cc")

func TestSubstringBetweenSeparators(t *testing.T) {
	require.Equal(t, []byte("a"), SubstringBetweenSeparators(testData, ',', 0))
	require.Equal(t, []byte("b"), SubstringBetweenSeparators(testData, ',', 1))
	require.Equal(t, []byte("c"), SubstringBetweenSeparators(testData, ',', 2))
	require.Equal(t, []byte("z"), SubstringBetweenSeparators(testData, ',', 25))
	require.Equal(t, []byte(nil), SubstringBetweenSeparators(testData, ',', 26))
	require.Equal(t, []byte(nil), SubstringBetweenSeparators(testData, ',', -1))

	require.Equal(t, []byte("aa"), SubstringBetweenSeparators(testData1, ',', 0))
	require.Equal(t, []byte(nil), SubstringBetweenSeparators(testData1, ',', 1))

	require.Equal(t, []byte("aa"), SubstringBetweenSeparators(testData2, ',', 0))
	require.Equal(t, []byte("bb"), SubstringBetweenSeparators(testData2, ',', 1))

	require.Equal(t, []byte("aa"), SubstringBetweenSeparators(testData3, ',', 0))
	require.Equal(t, []byte("cc"), SubstringBetweenSeparators(testData3, ',', 2))
	require.Equal(t, []byte(nil), SubstringBetweenSeparators(testData3, ',', 3))

}
