document.addEventListener('DOMContentLoaded', () => {
    
    // TAB SWITCHING LOGIC
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            // Remove active class from all buttons and contents
            tabBtns.forEach(b => b.classList.remove('active'));
            tabContents.forEach(c => c.classList.remove('active'));

            // Add active class to clicked button and corresponding content
            btn.classList.add('active');
            const targetId = btn.getAttribute('data-target');
            document.getElementById(targetId).classList.add('active');
        });
    });

    // SALARY TOGGLE LOGIC
    const salaryToggle = document.getElementById('salaryToggle');
    const salaryAmount = document.getElementById('salaryAmount');

    if (salaryToggle && salaryAmount) {
        salaryToggle.addEventListener('change', (e) => {
            if (e.target.checked) {
                salaryAmount.textContent = '$4,250.00';
                salaryAmount.style.opacity = '1';
            } else {
                salaryAmount.textContent = '****';
                salaryAmount.style.opacity = '0.5';
            }
        });
    }

    // MOBILE SIDEBAR TOGGLE
    const mobileToggle = document.getElementById('mobileToggle');
    const mobileClose = document.getElementById('mobileClose');
    const sidebar = document.querySelector('.sidebar');

    if (mobileToggle && sidebar) {
        mobileToggle.addEventListener('click', () => {
            sidebar.classList.add('open');
        });
    }

    if (mobileClose && sidebar) {
        mobileClose.addEventListener('click', () => {
            sidebar.classList.remove('open');
        });
    }

    // DUMMY TIMER (Just for visual effect)
    const timerElement = document.getElementById('digitalTimer');
    if (timerElement) {
        let seconds = 30214; // Start from 08:23:34
        
        setInterval(() => {
            seconds++;
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            const s = seconds % 60;
            
            const format = (num) => String(num).padStart(2, '0');
            timerElement.textContent = `${format(h)}:${format(m)}:${format(s)}`;
        }, 1000);
    }
});