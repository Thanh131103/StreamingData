import heapq
class Solution(object):
    def findMaximizedCapital(self, k, w, profits, capital):
        """
        :type k: int
        :type w: int
        :type profits: List[int]
        :type capital: List[int]
        :rtype: int
        """
        # Kết hợp profits và capital thành các cặp (capital, profit) và sắp xếp theo capital
        projects = sorted(zip(capital, profits))
        
        max_heap = []
        i = 0
        
        for _ in range(k):
            # Thêm tất cả các dự án có thể bắt đầu với vốn hiện tại vào heap
            while i < len(projects) and projects[i][0] <= w:
                heapq.heappush(max_heap, -projects[i][1])
                i += 1

            # Nếu không có dự án nào có thể bắt đầu, kết thúc
            if not max_heap:
                break

            # Bắt đầu dự án có lợi nhuận cao nhất
            w -= heapq.heappop(max_heap)
        
        return w
    
if __name__ == '__main__':
    k = 2 
    w = 0
    profits = [1,2,3] 
    capital = [0,1,1]
    s=Solution()
    ans=s.findMaximizedCapital(k, w, profits, capital)
    print(ans)